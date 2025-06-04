package raft

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors

import scala.io.StdIn

case class ClusterState(nodes: Map[String, ActorRef[RaftMessage]])

object RaftOrchestrator {
  def apply(): Behavior[RaftMessage] ={
    Behaviors.setup { context=>{
      var cluster = ClusterState(Map.empty)
      val initialBots = List("node1", "node2", "node3")
      //CREATE NEW BOT SERVERS, ADD THEM TO CLUSTER
      initialBots.foreach{ bot =>
        val newBot = context.spawn(RaftServer(bot,Map.empty), bot)
        cluster = cluster.copy(nodes = cluster.nodes + (bot->newBot))
      }
      //ASSIGN PEERS TO INITIAL BOTS
      val allNodeRefs = cluster.nodes

      cluster.nodes.foreach{
        case (id, ref) =>
            val assignedPeers = allNodeRefs.filter(x => x._1 != id)
            ref ! UpdatePeers(assignedPeers)
      }

      context.log.info("Raft cluster started with nodes: " + cluster.nodes.keys.mkString(", "))

      Behaviors.receiveMessage {
        case _ => Behaviors.same
      }

    }


    }
  }
}



object Main extends App {

  val raftNetwork = ActorSystem(RaftOrchestrator(),"akka_raft")
  println("Raft Cluster Started!")
  println("Commands:")
  println("  status - Show cluster status")
  println("  join <nodeId> - Add node to cluster")
  println("  leave <nodeId> - Remove node from cluster")
  println("  crash <nodeId> - Crash a node")
  println("  quit - Exit")
  var running = true

  while(running) {
    println("Waiting.....")
    Thread.sleep(100)
  }



}