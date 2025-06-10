package raft
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.javadsl.ActorContext
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration._
import scala.util.Random



object RaftServer {
  case class DemoConfig(
                         enableDetailedLogs: Boolean = false,
                         heartbeatInterval: FiniteDuration = 2.seconds,
                         electionTimeoutBase: FiniteDuration = 5.seconds,
                         commandDelay: FiniteDuration = 1.second
                       )

  val demoConfig = DemoConfig()

  def apply(nodeId: String, peers: Map[String,ActorRef[RaftMessage]]): Behavior[RaftMessage] = {
    Behaviors.withTimers { timers =>
      Behaviors.setup { context =>
        context.log.info(s"Starting Raft node: $nodeId")
        followerBehavior(
          nodeId = nodeId,
          currentTerm = 0,
          votedFor = None,
          peers = peers,
          leaderId = None,
          log = List.empty[LogEntry],
          timers = timers,
          commitIndex = 0,
          lastApplied = 0,
        )
      }
    }
  }
  private def lastLogIndex(log: List[LogEntry]): Int = log.length
  private def lastLogTerm(log: List[LogEntry]): Int = log.lastOption.map(_.term).getOrElse(0)
  private def getLogEntry(log: List[LogEntry], index: Int): Option[LogEntry] = {
    if (index > 0 && index <= log.length) Some(log(index - 1)) else None
  }
  private def getPrevLogTerm(log: List[LogEntry], index: Int): Int = {
    if (index <= 0) 0 else getLogEntry(log, index).map(_.term).getOrElse(0)
  }

  private def followerBehavior(nodeId: String,
                               currentTerm: Int,
                               votedFor: Option[String],
                               peers: Map[String, ActorRef[RaftMessage]],
                               leaderId: Option[String],
                               log: List[LogEntry],
                               commitIndex: Int,
                               lastApplied: Int,
                               timers: akka.actor.typed.scaladsl.TimerScheduler[RaftMessage]
                              ): Behavior[RaftMessage] = {
    Behaviors.setup { context =>

      val electionTimeout = demoConfig.electionTimeoutBase + Random.nextInt(2000).millis
      timers.startTimerWithFixedDelay("election-timeout", ElectionTimeout, electionTimeout)

      Behaviors.receiveMessage[RaftMessage]{
        case GetLogState(replyTo) =>
          replyTo ! LogStateResponse(nodeId, log, commitIndex, lastApplied, currentTerm, "FOLLOWER") // Change state accordingly
          Behaviors.same
        case ElectionTimeout =>
          context.log.info(s"[$nodeId] Election timeout - becoming candidate")
          // Cancel election timer before becoming candidate
          Thread.sleep(demoConfig.commandDelay.toMillis)
          timers.cancel("election-timeout")
          candidateBehavior(nodeId, peers, currentTerm + 1, log, commitIndex, lastApplied, timers)

        case Heartbeat(term, senderId) =>
          if (term >= currentTerm) {

            timers.cancel("election-timeout")
            val newElectionTimeout = demoConfig.electionTimeoutBase + Random.nextInt(2000).millis
            timers.startTimerWithFixedDelay("election-timeout", ElectionTimeout, newElectionTimeout)
            followerBehavior(nodeId, term, votedFor, peers, Some(senderId), log, commitIndex, lastApplied, timers)
          } else {
            context.log.info(s"[$nodeId] Ignoring old heartbeat from $senderId (term: $term, current: $currentTerm)")
            Behaviors.same
          }

        case UpdatePeers(newPeers) =>
          followerBehavior(nodeId, currentTerm, votedFor, newPeers, leaderId, log, commitIndex, lastApplied, timers)

        case RequestVote(term: Int, candidateId, replyTo) =>
          val shouldVote = term > currentTerm || (term == currentTerm && votedFor.isEmpty)

          if (shouldVote) {
            context.log.info(s"[$nodeId] Voting for $candidateId in term $term")
            replyTo ! VoteResponse(term, voteGranted = true)
            followerBehavior(nodeId, term, Some(candidateId), peers, leaderId, log, commitIndex, lastApplied, timers)
          } else {
            context.log.info(s"[$nodeId] Rejecting vote for $candidateId in term $term (already voted for: ${votedFor.getOrElse("none")})")
            replyTo ! VoteResponse(currentTerm, voteGranted = false)
            Behaviors.same
          }

        case CrashNode(crashNodeId) if crashNodeId == nodeId =>
          context.log.info(s"[$nodeId] CRASHING!")
          timers.cancel("election-timeout")
          Behaviors.stopped

        case ShowStatus =>
          context.log.info(s"[$nodeId] STATUS: FOLLOWER, Term: $currentTerm, Leader: ${leaderId.getOrElse("None")}")
          context.log.info(s"[$nodeId] Log: ${log.size} entries, commitIndex: $commitIndex, lastApplied: $lastApplied")
          peers.foreach { case (s, _) =>
            context.log.info(s"[$nodeId] has [$s] as a peer")
          }
          Behaviors.same

        case AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit, replyTo) =>
          if(term < currentTerm) {
            replyTo ! AppendEntriesResponse(currentTerm, success = false, lastLogIndex(log))
            Behaviors.same
          }else{
            timers.cancel("election-timeout")
            val newElectionTimeout = (3200.millis + Random.nextInt(500).millis)
            timers.startTimerWithFixedDelay("election-timeout", ElectionTimeout, newElectionTimeout)

            val logConsistent = if (prevLogIndex == 0) {
              true // Empty log is always consistent
            } else if (prevLogIndex > lastLogIndex(log)) {
              false // Don't have the previous entry
            } else {
              getPrevLogTerm(log, prevLogIndex) == prevLogTerm
            }

            if (logConsistent) {
              // Phase 1: Basic append without complex consistency checking
              val newLog = if (entries.nonEmpty) {
                context.log.info(s"[$nodeId] Appending ${entries.size} entries from leader $leaderId")
                entries.foreach(entry => println(s"   └─ Entry ${entry.index}: '${entry.command}' (term ${entry.term})"))
                Thread.sleep(demoConfig.commandDelay.toMillis)
                log ++ entries
              } else {
                context.log.debug(s"[$nodeId] Received heartbeat from leader $leaderId")
                log // Heartbeat with no entries
              }

              val newCommitIndex = math.min(leaderCommit, lastLogIndex(newLog))
              context.log.debug(s"[$nodeId] Updated commitIndex from $commitIndex to $newCommitIndex")

              replyTo ! AppendEntriesResponse(term, success = true, lastLogIndex(newLog))
              followerBehavior(nodeId, term, votedFor, peers, Some(leaderId), newLog, newCommitIndex, lastApplied, timers)
            } else {
              context.log.info(s"[$nodeId] Log inconsistency detected - prevLogIndex: $prevLogIndex, prevLogTerm: $prevLogTerm")
              replyTo ! AppendEntriesResponse(term, success = false, lastLogIndex(log))
              followerBehavior(nodeId, term, votedFor, peers, Some(leaderId), log, commitIndex, lastApplied, timers)
            }

          }

        case ClientCommand(command, replyTo) =>
          context.log.info(s"[$nodeId] Redirecting client command to leader: ${leaderId.getOrElse("unknown")}")
          replyTo ! ClientResponse(success = false, leaderId = leaderId)
          Behaviors.same

        case _ => Behaviors.same
      }
    }
  }

  private def candidateBehavior(nodeId: String,
                                peers: Map[String, ActorRef[RaftMessage]],
                                currentTerm: Int,
                                log: List[LogEntry],
                                commitIndex: Int,
                                lastApplied: Int,
                                timers: akka.actor.typed.scaladsl.TimerScheduler[RaftMessage]
                               ): Behavior[RaftMessage] = {
    Behaviors.setup { context =>
      context.log.info(s"[$nodeId] CANDIDATE - Starting election for term $currentTerm")

      var votesReceived = 1 // Vote for self
      val majorityNeeded = (peers.size + 1) / 2 + 1

      context.log.info(s"[$nodeId] Need $majorityNeeded votes to win (cluster size: ${peers.size + 1})")

      // Request votes from all peers
      peers.foreach { case (peerId, peerRef) =>
        context.log.info(s"[$nodeId] Requesting vote from $peerId")
        peerRef ! RequestVote(currentTerm, nodeId, context.self)
      }

      val electionTimeout = (250.millis + Random.nextInt(250).millis)
      timers.startTimerWithFixedDelay("election-timeout", ElectionTimeout, electionTimeout)



      def handleVotes(votes: Int): Behavior[RaftMessage] = {
        Behaviors.receiveMessage {
          case GetLogState(replyTo) =>
            replyTo ! LogStateResponse(nodeId, log, commitIndex, lastApplied, currentTerm, "FOLLOWER")
            Behaviors.same
          case VoteResponse(term, voteGranted) =>
            if (term > currentTerm) {
              context.log.info(s"[$nodeId] Higher term discovered: $term, becoming follower")
              timers.cancel("election-timeout")
              followerBehavior(nodeId, term, None, peers, None, log, commitIndex, lastApplied, timers)
            } else if (voteGranted && term == currentTerm) {
              val newVotes = votes + 1
              context.log.info(s"[$nodeId] Received vote! Total: $newVotes/$majorityNeeded needed")

              if (newVotes >= majorityNeeded) {
                context.log.info(s"[$nodeId]  WON ELECTION! Becoming leader for term $currentTerm")
                timers.cancel("election-timeout")
                Thread.sleep(demoConfig.commandDelay.toMillis)
                leaderBehavior(nodeId, currentTerm, peers, log, commitIndex, lastApplied, timers)
              } else {
                handleVotes(newVotes)
              }
            } else {
              context.log.info(s"[$nodeId] Vote rejected or for wrong term")
              handleVotes(votes)
            }
          case AppendEntries(term, leaderId, _, _, _, _, replyTo) =>
            if (term >= currentTerm) {
              context.log.info(s"[$nodeId] Valid leader $leaderId discovered during election, stepping down")
              replyTo ! AppendEntriesResponse(term, success = true, lastLogIndex(log))
              timers.cancel("election-timeout")
              Thread.sleep(demoConfig.commandDelay.toMillis)
              followerBehavior(nodeId, term, None, peers, Some(leaderId), log, commitIndex, lastApplied, timers)
            } else {
              context.log.info(s"[$nodeId] Rejecting AppendEntries from $leaderId (lower term: $term)")
              replyTo ! AppendEntriesResponse(currentTerm, success = false, lastLogIndex(log))
              Behaviors.same
            }

          case ElectionTimeout =>
            context.log.info(s"[$nodeId] Election timeout - starting new election")
            candidateBehavior(nodeId, peers, currentTerm + 1, log, commitIndex, lastApplied, timers)

          case RequestVote(term, candidateId, replyTo) =>
            if (term > currentTerm) {
              context.log.info(s"[$nodeId] Higher term discovered: $term, becoming follower and voting for $candidateId")
              replyTo ! VoteResponse(term, voteGranted = true)
              Thread.sleep(demoConfig.commandDelay.toMillis)
              timers.cancel("election-timeout")
              Thread.sleep(demoConfig.commandDelay.toMillis)
              followerBehavior(nodeId, term, Some(candidateId), peers, None, log, commitIndex, lastApplied, timers)
            } else {
              context.log.info(s"[$nodeId] Rejecting vote request from $candidateId (I'm candidate for term $currentTerm)")
              replyTo ! VoteResponse(currentTerm, voteGranted = false)
              Behaviors.same
            }

          case UpdatePeers(newPeers) =>
            context.log.info(s"[$nodeId] Updated peers during election")
            candidateBehavior(nodeId, newPeers, currentTerm, log, commitIndex, lastApplied, timers)

          case ShowStatus =>
            if (log.nonEmpty) {
              println(s"Recent entries:")
              log.takeRight(3).foreach { entry =>
                println(s"      [${entry.index}] '${entry.command}' (term ${entry.term})")
              }
            }
            Behaviors.same

          case CrashNode(crashNodeId) if crashNodeId == nodeId =>
            context.log.info(s"[$nodeId] CRASHING!")
            timers.cancel("election-timeout")
            Behaviors.stopped

          case _ => Behaviors.same
        }
      }

      handleVotes(votesReceived)
    }
  }

  private def leaderBehavior(nodeId: String,
                             currentTerm: Int,
                             peers: Map[String, ActorRef[RaftMessage]],
                             log: List[LogEntry],
                             commitIndex: Int,
                             lastApplied: Int,
                             timers: akka.actor.typed.scaladsl.TimerScheduler[RaftMessage]
                            ): Behavior[RaftMessage] = {
    Behaviors.setup { context =>
      context.log.info(s"[$nodeId] LEADER - Term $currentTerm, managing ${peers.size} followers")

      var nextIndex: Map[String, Int] = peers.keys.map(peerId => peerId -> (lastLogIndex(log) + 1)).toMap
      var matchIndex: Map[String, Int] = peers.keys.map(peerId => peerId -> 0).toMap

      peers.foreach { case (peerId, peerRef) =>
        val prevLogIndex = nextIndex.getOrElse(peerId, 1) - 1
        val prevLogTerm = getPrevLogTerm(log, prevLogIndex)
        peerRef ! AppendEntries(currentTerm, nodeId, prevLogIndex, prevLogTerm, List.empty, commitIndex, context.self)
      }

      timers.startTimerWithFixedDelay("heartbeat", SendHeartbeat, demoConfig.heartbeatInterval)

      def handleLeaderMessages(currentLog: List[LogEntry],
                               currentCommitIndex: Int,
                               currentNextIndex: Map[String, Int],
                               currentMatchIndex: Map[String, Int]): Behavior[RaftMessage] = {
        Behaviors.receiveMessage {
          case SendHeartbeat =>
            peers.foreach { case (peerId, peerRef) =>
              val prevLogIndex = currentNextIndex.getOrElse(peerId, 1) - 1
              val prevLogTerm = getPrevLogTerm(currentLog, prevLogIndex)
              peerRef ! AppendEntries(currentTerm, nodeId, prevLogIndex, prevLogTerm, List.empty, currentCommitIndex, context.self)
            }
            context.log.debug(s"[$nodeId] Sent heartbeats (AppendEntries) to ${peers.size} followers")
            Behaviors.same
          case GetLogState(replyTo) =>
            replyTo ! LogStateResponse(nodeId, log, commitIndex, lastApplied, currentTerm, "LEADER") // Change state accordingly
            Behaviors.same

          case ClientCommand(command, replyTo) =>
            context.log.info(s"[$nodeId] Received client command: $command")

            // Create new log entry
            val newEntry = LogEntry(
              term = currentTerm,
              index = lastLogIndex(currentLog) + 1,
              command = command
            )
            val newLog = currentLog :+ newEntry

            context.log.info(s"[$nodeId] Appended entry at index ${newEntry.index}: $command")

            // Send AppendEntries with new entry to all followers
            peers.foreach { case (peerId, peerRef) =>
              val prevLogIndex = currentNextIndex.getOrElse(peerId, 1) - 1
              val prevLogTerm = getPrevLogTerm(newLog, prevLogIndex)

              peerRef ! AppendEntries(
                term = currentTerm,
                leaderId = nodeId,
                prevLogIndex = prevLogIndex,
                prevLogTerm = prevLogTerm,
                entries = List(newEntry),
                leaderCommit = currentCommitIndex,
                replyTo = context.self
              )
            }

            replyTo ! ClientResponse(success = true, leaderId = Some(nodeId))

            handleLeaderMessages(newLog, currentCommitIndex, currentNextIndex, currentMatchIndex)

          case AppendEntriesResponse(term, success, matchIndexValue) =>
            if (term > currentTerm) {
              context.log.info(s"[$nodeId] Higher term discovered: $term, stepping down")
              timers.cancel("heartbeat")
              followerBehavior(nodeId, term, None, peers, None, currentLog, currentCommitIndex, lastApplied, timers)
            } else if (success) {
              context.log.debug(s"[$nodeId] Received successful AppendEntries response, matchIndex: $matchIndexValue")
              Behaviors.same
            } else {
              context.log.info(s"[$nodeId] Received failed AppendEntries response - will retry in next heartbeat")
              Behaviors.same
            }

          case RequestVote(term, candidateId, replyTo) =>
            if (term > currentTerm) {
              context.log.info(s"[$nodeId] Higher term discovered: $term, stepping down and voting for $candidateId")
              timers.cancel("heartbeat")
              replyTo ! VoteResponse(term, voteGranted = true)
              followerBehavior(nodeId, term, Some(candidateId), peers, None, currentLog, currentCommitIndex, lastApplied, timers)
            } else {
              context.log.info(s"[$nodeId] Rejecting vote request from $candidateId (I'm leader for term $currentTerm)")
              replyTo ! VoteResponse(currentTerm, voteGranted = false)
              Behaviors.same
            }

          case Heartbeat(term, senderId) =>
            if (term > currentTerm) {
              context.log.info(s"[$nodeId] Higher term heartbeat from $senderId: $term, stepping down")
              timers.cancel("heartbeat")
              followerBehavior(nodeId, term, None, peers, Some(senderId), currentLog, currentCommitIndex, lastApplied, timers)
            } else {
              context.log.info(s"[$nodeId] Ignoring heartbeat from $senderId (term: $term, I'm leader for term: $currentTerm)")
              Behaviors.same
            }

          case UpdatePeers(newPeers) =>
            context.log.info(s"[$nodeId] Leader updated peers: ${newPeers.size} followers")

            val newNextIndex = newPeers.keys.map(peerId => peerId -> (lastLogIndex(currentLog) + 1)).toMap
            val newMatchIndex = newPeers.keys.map(peerId => peerId -> 0).toMap

            timers.cancel("heartbeat")
            leaderBehavior(nodeId, currentTerm, newPeers, currentLog, currentCommitIndex, lastApplied, timers)

          case ShowStatus =>
            context.log.info(s"[$nodeId] STATUS: LEADER, Term: $currentTerm, Followers: ${peers.size}")
            context.log.info(s"[$nodeId] Log: ${currentLog.size} entries, commitIndex: $currentCommitIndex, lastApplied: $lastApplied")
            if (currentLog.nonEmpty) {
              println(s"   📋 Recent entries:")
              currentLog.takeRight(3).foreach { entry =>
                println(s"      [${entry.index}] '${entry.command}' (term ${entry.term})")
              }
            }
            Behaviors.same

          case CrashNode(crashNodeId) if crashNodeId == nodeId =>
            context.log.info(s"[$nodeId] CRASHING!")
            timers.cancel("heartbeat")
            Behaviors.stopped

          case _ => Behaviors.same
        }
      }

      handleLeaderMessages(log, commitIndex, nextIndex, matchIndex)
    }
  }
}