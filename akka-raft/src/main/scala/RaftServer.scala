package raft
import akka.actor.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.ActorRef
import akka.actor.typed.javadsl.ActorContext

import scala.util.Random

trait RaftMessage
case object ElectionTimeout extends RaftMessage



object RaftServer {

  def apply(nodeId: String, peers: Set[ActorRef]): Behavior[RaftMessage] = {
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
                               votedFor: None.type,
                               peers: Set[ActorRef],
                               leaderId: None.type
                              ): Behavior[RaftMessage] = {
    Behaviors.setup { context =>
      val electionTimeout = (150.millis +  Random.nextInt(150).millis)
      context.scheduleOnce(electionTimeout, context.self, ElectionTimeout)
      context.log.info(s"[$nodeId] FOLLOWER - Term: $currentTerm, Leader: ${leaderId.getOrElse("None")}")

      Behaviors.receiveMessage[RaftMessage]{
        case ElectionTimeout =>
          context.log.info(s"[$nodeId] Election timeout - becoming candidate")
          Behaviors.same
      }
    }

  }
}