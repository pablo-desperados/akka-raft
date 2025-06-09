package raft
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.javadsl.ActorContext
import scala.concurrent.duration.DurationInt

import scala.util.Random

object RaftServer {

  def apply(nodeId: String, peers: Map[String,ActorRef[RaftMessage]]): Behavior[RaftMessage] = {
    Behaviors.setup { context =>
      context.log.info(s"Starting Raft node: $nodeId")
      followerBehavior(
        nodeId = nodeId,
        currentTerm = 0,
        votedFor = None,
        peers = peers,
        leaderId = None
      )
    }
  }


  private def followerBehavior(nodeId: String,
                               currentTerm: Int,
                               votedFor: Option[String],
                               peers: Map[String, ActorRef[RaftMessage]],
                               leaderId: Option[String]
                              ): Behavior[RaftMessage] = {
    Behaviors.setup { context =>
      val electionTimeout = (250.millis +  Random.nextInt(250).millis)
      context.scheduleOnce(electionTimeout, context.self, ElectionTimeout)
      context.log.info(s"[$nodeId] FOLLOWER - Term: $currentTerm, Leader: ${leaderId.getOrElse("None")}")

      Behaviors.receiveMessage[RaftMessage]{
        case ElectionTimeout =>
          context.log.info(s"[$nodeId] Election timeout - becoming candidate")
          candidateBehavior(nodeId,peers, currentTerm+1)

        case UpdatePeers(newPeers) =>
          followerBehavior(nodeId, currentTerm, votedFor, newPeers, leaderId)

        case CrashNode(crashNodeId) if crashNodeId == nodeId =>
            context.log.info(s"[$nodeId] CRASHING!")
            Behaviors.stopped
        case RequestVote(term:Int, candidateId, replyTo) =>
          val shouldVote = term > currentTerm || (term == currentTerm && votedFor.isEmpty)

          if (shouldVote) {
            context.log.info(s"[$nodeId] Voting for $candidateId in term $term")
            replyTo ! VoteResponse(term, voteGranted = true)
            followerBehavior(nodeId, term, votedFor, peers, leaderId)
          } else {
            context.log.info(s"[$nodeId] Rejecting vote for $candidateId in term $term")
            replyTo ! VoteResponse(currentTerm, voteGranted = false)
            Behaviors.same
          }
        case ShowStatus=>
          peers.foreach{
            case (s,_)=>
              context.log.info(s"[$nodeId] has [$s] as a peer")
          }
          Behaviors.same

      }
    }

  }


  private def candidateBehavior(nodeId: String, peers: Map[String, ActorRef[RaftMessage]], currentTerm: Int): Behavior[RaftMessage] = {
    Behaviors.setup { context =>
      context.log.info(s"[$nodeId] CANDIDATE - Starting election for term $currentTerm")

      var votesReceived = 1
      val majorityNeeded = (peers.size + 1) / 2 + 1

      context.log.info(s"[$nodeId] Need $majorityNeeded votes to win (cluster size: ${peers.size + 1})")

      peers.foreach { case (peerId, peerRef) =>
        context.log.info(s"[$nodeId] Requesting vote from $peerId")
        peerRef ! RequestVote(currentTerm, nodeId, context.self)
      }

      val electionTimeout = (250.millis + Random.nextInt(250).millis)
      context.scheduleOnce(electionTimeout, context.self, ElectionTimeout)

      def handleVotes(votes: Int): Behavior[RaftMessage] = {
        Behaviors.receiveMessage {
          case VoteResponse(term, voteGranted) =>
            if (term > currentTerm) {
              context.log.info(s"[$nodeId] Higher term discovered: $term, becoming follower")
              followerBehavior(nodeId, term, None, peers, None)
            } else if (voteGranted && term == currentTerm) {
              val newVotes = votes + 1
              context.log.info(s"[$nodeId] Received vote! Total: $newVotes/$majorityNeeded needed")

              if (newVotes >= majorityNeeded) {
                context.log.info(s"[$nodeId] 🎉 WON ELECTION! Becoming leader for term $currentTerm")
//                leaderBehavior(nodeId, currentTerm, peers)
                Behaviors.same
              } else {
                handleVotes(newVotes)
              }
            } else {
              context.log.info(s"[$nodeId] Vote rejected or for wrong term")
              handleVotes(votes)
            }

          case ElectionTimeout =>
            context.log.info(s"[$nodeId] Election timeout - starting new election")
            candidateBehavior(nodeId, peers, currentTerm + 1)

          case RequestVote(term, candidateId, replyTo) =>
            if (term > currentTerm) {
              context.log.info(s"[$nodeId] Higher term discovered: $term, becoming follower and voting for $candidateId")
              replyTo ! VoteResponse(term, voteGranted = true)
              followerBehavior(nodeId, term, Some(candidateId), peers, None)
            } else {
              context.log.info(s"[$nodeId] Rejecting vote request from $candidateId (I'm candidate for term $currentTerm)")
              replyTo ! VoteResponse(currentTerm, voteGranted = false)
              Behaviors.same
            }

          case UpdatePeers(newPeers) =>
            context.log.info(s"[$nodeId] Updated peers during election")
            candidateBehavior(nodeId, newPeers, currentTerm)

          case ShowStatus =>
            context.log.info(s"[$nodeId] STATUS: CANDIDATE, Term: $currentTerm, Votes: $votes/$majorityNeeded")
            Behaviors.same

          case CrashNode(crashNodeId) if crashNodeId == nodeId =>
            context.log.info(s"[$nodeId] CRASHING!")
            Behaviors.stopped

          case _ => Behaviors.same
        }
      }

      handleVotes(votesReceived)
    }
  }


}