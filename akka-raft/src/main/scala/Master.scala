import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes
import raft.{RaftOrchestrator,JoinCluster,LeaveCluster,ShowStatus,CrashNode}
import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn
import scala.util.{Success, Failure}

object Master extends App {

  //CREATE SYSTEM
  implicit val system: ActorSystem[raft.RaftMessage] = ActorSystem(RaftOrchestrator(), "raft-master")
  implicit val executionContext: ExecutionContextExecutor = system.executionContext

  //CREATE ROUTES FOR HANDLING JOINING/LEAVING NETWORK REMOTELY
  val route =
    pathPrefix("raft") {
      path("join" / Segment) { nodeId =>
        post {
          println(s"[REMOTE] Node $nodeId requesting to join")
          system ! JoinCluster(nodeId)
          complete(StatusCodes.OK, s"Node $nodeId join request sent")
        }
      } ~
        path("leave" / Segment) { nodeId =>
          post {
            println(s"[REMOTE] Node $nodeId requesting to leave")
            system ! LeaveCluster(nodeId)
            complete(StatusCodes.OK, s"Node $nodeId leave request sent")
          }
        } ~
        path("status") {
          get {
            system ! ShowStatus
            complete(StatusCodes.OK, "Status request sent")
          }
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