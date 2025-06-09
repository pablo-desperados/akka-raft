package raft
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.javadsl.ActorContext
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration._
import scala.util.Random



object RaftServer {

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
          timers = timers
        )
      }
    }
  }

  private def followerBehavior(nodeId: String,
                               currentTerm: Int,
                               votedFor: Option[String],
                               peers: Map[String, ActorRef[RaftMessage]],
                               leaderId: Option[String],
                               timers: akka.actor.typed.scaladsl.TimerScheduler[RaftMessage]
                              ): Behavior[RaftMessage] = {
    Behaviors.setup { context =>
      val electionTimeout = (3200.millis + Random.nextInt(500).millis)
      timers.startTimerWithFixedDelay("election-timeout", ElectionTimeout, electionTimeout)

      Behaviors.receiveMessage[RaftMessage]{
        case ElectionTimeout =>
          context.log.info(s"[$nodeId] Election timeout - becoming candidate")
          // Cancel election timer before becoming candidate
          timers.cancel("election-timeout")
          candidateBehavior(nodeId, peers, currentTerm + 1, timers)

        case Heartbeat(term, senderId) =>
          if (term >= currentTerm) {

            timers.cancel("election-timeout")
            val newElectionTimeout = (3200.millis + Random.nextInt(500).millis)
            timers.startTimerWithFixedDelay("election-timeout", ElectionTimeout, newElectionTimeout)
            followerBehavior(nodeId, term, votedFor, peers, Some(senderId), timers)
          } else {
            context.log.info(s"[$nodeId] Ignoring old heartbeat from $senderId (term: $term, current: $currentTerm)")
            Behaviors.same
          }

        case UpdatePeers(newPeers) =>
          followerBehavior(nodeId, currentTerm, votedFor, newPeers, leaderId, timers)

        case RequestVote(term: Int, candidateId, replyTo) =>
          val shouldVote = term > currentTerm || (term == currentTerm && votedFor.isEmpty)

          if (shouldVote) {
            context.log.info(s"[$nodeId] Voting for $candidateId in term $term")
            replyTo ! VoteResponse(term, voteGranted = true)
            followerBehavior(nodeId, term, Some(candidateId), peers, leaderId, timers)
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
          peers.foreach { case (s, _) =>
            context.log.info(s"[$nodeId] has [$s] as a peer")
          }
          Behaviors.same

        case _ => Behaviors.same
      }
    }
  }

  private def candidateBehavior(nodeId: String,
                                peers: Map[String, ActorRef[RaftMessage]],
                                currentTerm: Int,
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
          case VoteResponse(term, voteGranted) =>
            if (term > currentTerm) {
              context.log.info(s"[$nodeId] Higher term discovered: $term, becoming follower")
              timers.cancel("election-timeout")
              followerBehavior(nodeId, term, None, peers, None, timers)
            } else if (voteGranted && term == currentTerm) {
              val newVotes = votes + 1
              context.log.info(s"[$nodeId] Received vote! Total: $newVotes/$majorityNeeded needed")

              if (newVotes >= majorityNeeded) {
                context.log.info(s"[$nodeId]  WON ELECTION! Becoming leader for term $currentTerm")
                timers.cancel("election-timeout")
                leaderBehavior(nodeId, currentTerm, peers, timers)
              } else {
                handleVotes(newVotes)
              }
            } else {
              context.log.info(s"[$nodeId] Vote rejected or for wrong term")
              handleVotes(votes)
            }

          case ElectionTimeout =>
            context.log.info(s"[$nodeId] Election timeout - starting new election")
            candidateBehavior(nodeId, peers, currentTerm + 1, timers)

          case RequestVote(term, candidateId, replyTo) =>
            if (term > currentTerm) {
              context.log.info(s"[$nodeId] Higher term discovered: $term, becoming follower and voting for $candidateId")
              replyTo ! VoteResponse(term, voteGranted = true)
              timers.cancel("election-timeout")
              followerBehavior(nodeId, term, Some(candidateId), peers, None, timers)
            } else {
              context.log.info(s"[$nodeId] Rejecting vote request from $candidateId (I'm candidate for term $currentTerm)")
              replyTo ! VoteResponse(currentTerm, voteGranted = false)
              Behaviors.same
            }

          case UpdatePeers(newPeers) =>
            context.log.info(s"[$nodeId] Updated peers during election")
            candidateBehavior(nodeId, newPeers, currentTerm, timers)

          case ShowStatus =>
            context.log.info(s"[$nodeId] STATUS: CANDIDATE, Term: $currentTerm, Votes: $votes/$majorityNeeded")
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
                             timers: akka.actor.typed.scaladsl.TimerScheduler[RaftMessage]
                            ): Behavior[RaftMessage] = {
    Behaviors.setup { context =>
      context.log.info(s"[$nodeId] LEADER - Term $currentTerm, managing ${peers.size} followers")

      // Send initial heartbeat immediately
      peers.foreach { case (peerId, peerRef) =>
        peerRef ! Heartbeat(currentTerm, nodeId)
      }

      val heartbeatInterval = 100.millis
      timers.startTimerWithFixedDelay("heartbeat", SendHeartbeat, heartbeatInterval)

      Behaviors.receiveMessage {
        case SendHeartbeat =>
          // Send heartbeats to all followers
          peers.foreach { case (peerId, peerRef) =>
            peerRef ! Heartbeat(currentTerm, nodeId)
          }
          context.log.debug(s"[$nodeId] Sent heartbeats to ${peers.size} followers")
          Behaviors.same

        case RequestVote(term, candidateId, replyTo) =>
          if (term > currentTerm) {
            context.log.info(s"[$nodeId] Higher term discovered: $term, stepping down and voting for $candidateId")
            timers.cancel("heartbeat")
            replyTo ! VoteResponse(term, voteGranted = true)
            followerBehavior(nodeId, term, Some(candidateId), peers, None, timers)
          } else {
            context.log.info(s"[$nodeId] Rejecting vote request from $candidateId (I'm leader for term $currentTerm)")
            replyTo ! VoteResponse(currentTerm, voteGranted = false)
            Behaviors.same
          }

        case Heartbeat(term, senderId) =>
          if (term > currentTerm) {
            context.log.info(s"[$nodeId] Higher term heartbeat from $senderId: $term, stepping down")
            timers.cancel("heartbeat")
            followerBehavior(nodeId, term, None, peers, Some(senderId), timers)
          } else {
            context.log.info(s"[$nodeId] Ignoring heartbeat from $senderId (term: $term, I'm leader for term: $currentTerm)")
            Behaviors.same
          }

        case UpdatePeers(newPeers) =>
          context.log.info(s"[$nodeId] Leader updated peers: ${newPeers.size} followers")
          timers.cancel("heartbeat")
          leaderBehavior(nodeId, currentTerm, newPeers, timers)

        case ShowStatus =>
          context.log.info(s"[$nodeId] STATUS: LEADER, Term: $currentTerm, Followers: ${peers.size}")
          Behaviors.same

        case CrashNode(crashNodeId) if crashNodeId == nodeId =>
          context.log.info(s"[$nodeId] CRASHING!")
          timers.cancel("heartbeat")
          Behaviors.stopped

        case _ => Behaviors.same
      }
    }
  }
}