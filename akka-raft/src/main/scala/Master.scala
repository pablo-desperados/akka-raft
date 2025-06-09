import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import raft.{CrashNode, JoinCluster, LeaveCluster, RaftOrchestrator, ShowStatus}

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn
import scala.util.{Failure, Success}
import collection.mutable
import scala.concurrent.duration.DurationInt
case class NodeInfo(lastHeartbeat: Long, joinTime: Long)
object Master extends App {

  //CREATE SYSTEM
  implicit val system: ActorSystem[raft.RaftMessage] = ActorSystem(RaftOrchestrator(), "raft-master")
  implicit val executionContext: ExecutionContextExecutor = system.executionContext

  val nodeHeartbeats = mutable.Map[String, NodeInfo]()

  //CREATE ROUTES FOR HANDLING JOINING/LEAVINeG NETWORK REMOTELY
  val route =
    pathPrefix("raft") {
      path("join" / Segment) { nodeId =>
        post {
          println(s"[REMOTE] Node $nodeId requesting to join")
          val now = System.currentTimeMillis()
          nodeHeartbeats.put(nodeId, NodeInfo(now, now))
          system ! JoinCluster(nodeId)
          complete(StatusCodes.OK, s"Node $nodeId join request sent")
        }
      } ~
        path("leave" / Segment) { nodeId =>
          post {
            println(s"[REMOTE] Node $nodeId requesting to leave")
            nodeHeartbeats.remove(nodeId)
            system ! LeaveCluster(nodeId)
            complete(StatusCodes.OK, s"Node $nodeId leave request sent")
          }
        } ~ path( "promote"/Segment){nodeId =>{
          post{
            println("Hello")
            complete(StatusCodes.OK, s"Node $nodeId leave request sent")
          }
      }

      }~ path("heartbeat" / Segment) { nodeId =>
          post {
            val now = System.currentTimeMillis()
            nodeHeartbeats.get(nodeId) match {
              case Some(info) =>
                // Update heartbeat timestamp
                nodeHeartbeats.put(nodeId, info.copy(lastHeartbeat = now))
                complete(StatusCodes.OK, s"Heartbeat from $nodeId received")
              case None =>
                // Node not in cluster, reject heartbeat
                complete(StatusCodes.NotFound, s"Node $nodeId not found in cluster")
            }
            complete(StatusCodes.OK, HttpEntity(ContentTypes.`text/plain(UTF-8)`, s"Heartbeat from '$nodeId' received"))
          }

        } ~ path("status") {
        get {
          system ! ShowStatus
          complete(StatusCodes.OK, "Status request sent")
        }
      } ~ pathEndOrSingleSlash {
        get {
          complete(StatusCodes.OK,
            HttpEntity(ContentTypes.`text/plain(UTF-8)`,
              """🚀 RAFT Cluster API
                |""".stripMargin))
        }
      } ~ pathEndOrSingleSlash {
        get {
          complete(StatusCodes.OK,
            HttpEntity(ContentTypes.`text/plain(UTF-8)`,
              "🚀 RAFT Master Server Running! Visit /raft for API info"))
        }
      }
    }

  system.scheduler.scheduleAtFixedRate(5.seconds, 5.seconds) { () =>
    val now = System.currentTimeMillis()
    val staleThreshold = 10000 // 10 seconds

    val staleNodes = nodeHeartbeats.filter { case (nodeId, info) =>
      (now - info.lastHeartbeat) > staleThreshold
    }.keys.toList

    staleNodes.foreach { nodeId =>
      val info = nodeHeartbeats(nodeId)
      val secondsStale = (now - info.lastHeartbeat) / 1000
      println(s"[FAILURE DETECTION] Removing stale node: $nodeId (no heartbeat for ${secondsStale}s)")

      // Remove from tracking and cluster
      nodeHeartbeats.remove(nodeId)
      system ! LeaveCluster(nodeId)
    }
  }

  val bindingFuture = Http().newServerAt("localhost", 8080).bind(route)
  bindingFuture.onComplete {
    case Success(binding) =>
      println(s"🚀 RAFT MASTER SERVER STARTED")
      println(s"📡 HTTP API running at: http://localhost:8080")
      println(s"💻 Master Console ready")
    case Failure(exception) =>
      println(s"Failed to start HTTP server: $exception")
      system.terminate()
  }
  println("Raft Cluster Started!")
  println("Commands:")
  println("  status - Show cluster status")
  println("  join <nodeId> - Add node to cluster")
  println("  leave <nodeId> - Remove node from cluster")
  println("  crash <nodeId> - Crash a node")
  println("  quit - Exit")
  var running = true
  while (running) {
    print("> ")
    val input = StdIn.readLine().trim.split(" ")

    input.headOption match {
      case Some("status") =>
        system ! ShowStatus

      case Some("quit") =>
        running = false

      case Some("crash") if input.length > 1 =>
        val nodeId = input(1)
        println(s"Crashing node: $nodeId")
        system ! CrashNode(nodeId)
      case _ =>
        println("Invalid command")
    }
  }
  bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate())

}