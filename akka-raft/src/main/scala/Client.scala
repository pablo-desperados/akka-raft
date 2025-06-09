import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpMethods}
import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn
import scala.concurrent.duration._
import scala.util.{Success, Failure, Random}

object Client extends App {
  val nodeId = args(0)
  val masterHost = if (args.length > 1) args(1) else "localhost"
  val masterUrl = s"http://$masterHost:8080"
  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, s"client-$nodeId")
  implicit val executionContext: ExecutionContextExecutor = system.executionContext

  def sendRequest(endpoint: String): Unit = {
    val url = s"$masterUrl/raft/$endpoint"
    val request = HttpRequest(HttpMethods.POST, uri = url)

    Http().singleRequest(request).onComplete {
      case Success(response) =>
        if (response.status.isSuccess()) {
          println(s"Success: ${response.status}")
        } else {
          println(s"Failed: ${response.status}")
        }
      case Failure(exception) =>

        println(s"Failure. Is the master server running on $masterHost:8080?")
    }
  }
  def sendHeartbeat(): Unit = {
    sendRequest(s"heartbeat/$nodeId")
  }



  println(s"Testing connection to master at $masterHost...")
  Http().singleRequest(HttpRequest(HttpMethods.GET, uri = s"$masterUrl/raft")).onComplete {

    case Success(response) =>
      if (response.status.isSuccess()) {
        println(s"Connected to master at $masterHost")
      } else {
        println(s"Master responded with: ${response.status}")
      }
    case Failure(exception) =>
      println(s"Cannot reach master: ${exception.getMessage}")
      println(s"Make sure master is running: sbt \"runMain HttpMaster\"")
  }

  println(s"Starting CLIENT Server: [$nodeId]")
  var running = true
  var hasJoined = false
  var heartbeatScheduler: Option[akka.actor.Cancellable] = None

  def stopHeartbeat(): Unit = {
    heartbeatScheduler.foreach(_.cancel())
    heartbeatScheduler = None
    println("Stopped automatic heartbeat")
  }

  def startHeartbeat(): Unit = {
    import scala.concurrent.duration._
    heartbeatScheduler = Some(
      system.scheduler.scheduleAtFixedRate(3.seconds, 3.seconds) { () =>
        sendHeartbeat()
      }
    )
    println("Started automatic heartbeat (every 3 seconds)")
  }
  while (running) {

    val prompt = if (hasJoined) s"$nodeId@$masterHost> " else s"$nodeId (disconnected)> "
    print(prompt)

    val input = StdIn.readLine().trim

    input match {

      case "join" =>
        if (!hasJoined) {
          println(s"Requesting to join cluster '$masterHost' as '$nodeId'...")
          sendRequest(s"join/$nodeId")
          hasJoined = true
          startHeartbeat()
        } else {
          println("⚠Already joined the cluster")
        }

      case "leave" =>
        if (hasJoined) {
          println(s"Leaving cluster...")
          sendRequest(s"leave/$nodeId")
          hasJoined = false
          stopHeartbeat()
        } else {
          println("Not in cluster")
        }
    }
  }

  sys.addShutdownHook {
    if (hasJoined) {
      println("Automatically leaving cluster...")
      import scala.concurrent.Await
      import scala.concurrent.duration._
      try {
        val request = HttpRequest(HttpMethods.POST, uri = s"$masterUrl/raft/leave/$nodeId")
        Await.result(Http().singleRequest(request), 5.seconds)
      } catch {
        case _:Exception =>
          println("Could not reach master during shutdown")
      }
    }
    stopHeartbeat()
    system.terminate()
  }

  system.terminate()
}