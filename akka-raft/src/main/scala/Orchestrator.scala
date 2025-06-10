package raft

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors

import scala.io.StdIn

case class ClusterState(nodes: Map[String, ActorRef[RaftMessage]])
case class NodeInfo(lastHeartbeat: Long, joinTime: Long)
case class GetCurrentLeader(replyTo: ActorRef[Option[ActorRef[RaftMessage]]]) extends RaftMessage
case class GetClusterStatus(replyTo: ActorRef[String]) extends RaftMessage
import akka.actor.typed.scaladsl.AskPattern._
import scala.concurrent.Future
import scala.util.{Success, Failure}



object RaftOrchestrator {


  private def displayLogVisualization(responses: List[LogStateResponse]): Unit = {
    println("\n" + "="*80)
    println("====== RAFT LOG VISUALIZATION =======")
    println("="*80)

    val maxLogLength = if (responses.nonEmpty) responses.map(_.log.length).max else 0
    val maxIndex = math.max(10, maxLogLength) // Show at least 10 positions

    // Header with indices
    print("Node    ")
    for (i <- 1 to maxIndex) {
      print(f"$i%3d")
    }
    println(s"  | Commit | Applied | Term | State")
    println("-" * (8 + maxIndex * 3 + 35))

    // Sort responses by nodeId for consistent display
    responses.sortBy(_.nodeId).foreach { response =>
      val LogStateResponse(nodeId, log, commitIndex, lastApplied, currentTerm, state) = response

      print(f"$nodeId%-7s ")


      for (i <- 1 to maxIndex) {
        if (i <= log.length) {
          val entry = log(i - 1)
          val symbol = if (i <= commitIndex) "[X]" else "[]"
          val termColor = getTermColor(entry.term)
          print(f"$termColor${entry.term}$symbol%-2s")
        } else {
          print("   ")  // Empty slot
        }
      }

      val stateIcon = state match {
        case "LEADER" => "O"
        case "CANDIDATE" => "X"
        case _ => "  "
      }

      println(f"  | $commitIndex%6d | $lastApplied%7d | $currentTerm%4d | $state $stateIcon")
    }

    println("\nLegend:")
    println("[X] = Committed entry, [] = Uncommitted entry")
    println("  Numbers = Term numbers")
    println("  O = Leader    X️ = Candidate")
    println("="*80)
  }

  def getTermColor(term: Int): String = {
    term % 6 match {
      case 0 => Console.RED
      case 1 => Console.GREEN
      case 2 => Console.YELLOW
      case 3 => Console.BLUE
      case 4 => Console.MAGENTA
      case 5 => Console.CYAN
      case _ => Console.WHITE
    }
  }
  def apply(): Behavior[RaftMessage] = {
    Behaviors.setup { context =>
      implicit val ec = context.executionContext
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

        case SubmitCommand(command) =>
          context.log.info(s"Broadcasting to ${cluster.nodes.size} nodes")

          // Send ClientCommand to all nodes - the leader will process it, followers will redirect
          cluster.nodes.foreach { case (nodeId, nodeRef) =>
            nodeRef ! ClientCommand(command, context.system.ignoreRef)
          }
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

        case GetCurrentLeader(replyTo) =>
          val leader = cluster.nodes.headOption.map(_._2)
          replyTo ! leader
          Behaviors.same

        case GetClusterStatus(replyTo) =>
          val status = s"Cluster Status: ${cluster.nodes.size} nodes (${cluster.nodes.keys.mkString(", ")})"
          replyTo ! status
          Behaviors.same

        case ShowLogs() =>
          context.log.info("=== REQUESTING LOG STATE FROM ALL NODES ===")

          import akka.actor.typed.scaladsl.AskPattern._
          import scala.concurrent.duration._
          import scala.util.{Success, Failure}
          import scala.concurrent.Future

          // Ask all nodes for their log state
          val logRequests: List[Future[LogStateResponse]] = cluster.nodes.map { case (nodeId, nodeRef) =>
            nodeRef.ask(ref => GetLogState(ref))(3.seconds, context.system.scheduler)
          }.toList

          // When all responses come back, display the visualization
          val allLogs = Future.sequence(logRequests)

          allLogs.onComplete {
            case Success(responses) =>
              displayLogVisualization(responses)
            case Failure(ex) =>
              context.log.error(s"Failed to get log states: $ex")
          }(context.executionContext)

          Behaviors.same

          // Add this helper method to RaftOrchestrator:

        case _ =>
          context.log.warn("Orchestrator received unhandled message")
          Behaviors.same

      }
    }
  }
}