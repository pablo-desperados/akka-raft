package raft

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors

import scala.io.StdIn

case class ClusterState(nodes: Map[String, ActorRef[RaftMessage]])
case class NodeInfo(lastHeartbeat: Long, joinTime: Long)



object RaftOrchestrator {
  def apply(): Behavior[RaftMessage] = {
    Behaviors.setup { context =>
      var cluster = ClusterState(Map.empty)
      val initialBots = List("node1", "node2", "node3")

      // CREATE NEW BOT SERVERS, ADD THEM TO CLUSTER
      initialBots.foreach { bot =>
        val newBot = context.spawn(RaftServer(bot, Map.empty), bot)
        cluster = cluster.copy(nodes = cluster.nodes + (bot -> newBot))
      }

      // ASSIGN PEERS TO INITIAL BOTS
      val allNodeRefs = cluster.nodes

      cluster.nodes.foreach {
        case (id, ref) =>
          val assignedPeers = allNodeRefs.filter(x => x._1 != id)
          ref ! UpdatePeers(assignedPeers)
      }

      context.log.info("Raft cluster started with nodes: " + cluster.nodes.keys.mkString(", "))

      Behaviors.receiveMessage {
        case ShowStatus =>
          context.log.info("=== CLUSTER STATUS ===")
          cluster.nodes.values.foreach(_ ! ShowStatus)
          Behaviors.same

        case JoinCluster(nodeId) =>
          if (cluster.nodes.contains(nodeId)) {
            context.log.warn(s"Node $nodeId already exists in cluster")
          } else {
            context.log.info(s"=== ADDING NODE: $nodeId ===")
            // Create new node with current peers
            val currentPeers = cluster.nodes.filter(_._1 != nodeId)
            val newNode = context.spawn(RaftServer(nodeId, currentPeers), nodeId)

            // Add to cluster
            cluster = cluster.copy(nodes = cluster.nodes + (nodeId -> newNode))

            // Update ALL nodes (including new one) with complete peer list
            val allNodeRefs = cluster.nodes
            cluster.nodes.foreach {
              case (id, ref) =>
                val assignedPeers = allNodeRefs.filter(x => x._1 != id)
                ref ! UpdatePeers(assignedPeers)
            }

            context.log.info(s"Node $nodeId joined. Cluster nodes: ${cluster.nodes.keys.mkString(", ")}")
          }
          Behaviors.same

        case LeaveCluster(nodeId) =>
          cluster.nodes.get(nodeId) match {
            case Some(nodeRef) =>
              context.log.info(s"=== REMOVING NODE: $nodeId ===")
              context.stop(nodeRef)
              cluster = cluster.copy(nodes = cluster.nodes - nodeId)
              val updatedNodeRefs = cluster.nodes
              cluster.nodes.foreach {
                case (id, ref) =>
                  val assignedPeers = updatedNodeRefs.filter(x => x._1 != id)
                  ref ! UpdatePeers(assignedPeers)
              }
              context.log.info(s"Node $nodeId left. Remaining nodes: ${cluster.nodes.keys.mkString(", ")}")
            case None =>
              context.log.info(s"Cannot remove node $nodeId - node not found")
          }
          Behaviors.same

        case CrashNode(nodeId: String)=>
          cluster.nodes.get(nodeId) match {
            case Some(node)=>
              context.log.info(s"=== CRASHING NODE: $nodeId ===")
              node ! CrashNode(nodeId)
              cluster = cluster.copy(nodes = cluster.nodes - nodeId)
              val updatedNodeRefs = cluster.nodes
              cluster.nodes.foreach{
                case (id, ref)=>
                  val assignedPeers = updatedNodeRefs.filter(x => x._1 != id)
                  ref ! UpdatePeers(assignedPeers)
              }

            case None=>
              context.log.info(s"Cannot crash node $nodeId - node not found")
          }
          Behaviors.same

        case _ =>
          context.log.warn("Orchestrator received unhandled message")
          Behaviors.same
      }
    }
  }
}