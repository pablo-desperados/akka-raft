import akka.actor.typed.ActorSystem
import raft.{RaftOrchestrator, ShowStatus, CrashNode}
import scala.io.StdIn

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
  while (running) {
    print("> ")
    val input = StdIn.readLine().trim.split(" ")

    input.headOption match {
      case Some("status") =>
        raftNetwork ! ShowStatus

      case Some("quit") =>
        running = false

      case Some("crash") if input.length > 1 =>
        val nodeId = input(1)
        println(s"Crashing node: $nodeId")
        raftNetwork ! CrashNode(nodeId)
      case _ =>
        println("Invalid command")
    }
  }
  raftNetwork.terminate()



}