package raft
import akka.actor.typed.ActorRef

trait RaftMessage
case object ElectionTimeout extends RaftMessage
case class UpdatePeers(peers: Map[String, ActorRef[RaftMessage]]) extends RaftMessage
case object ShowStatus extends RaftMessage
case class CrashNode(nodeId: String) extends RaftMessage
case class JoinCluster(nodeId: String) extends RaftMessage
case class LeaveCluster(nodeId: String) extends RaftMessage
case class RequestVote(term: Int, candidateId: String, replyTo: ActorRef[VoteResponse]) extends RaftMessage
case class VoteResponse(term: Int, voteGranted: Boolean) extends RaftMessage
case object SendHeartbeat extends RaftMessage
case class Heartbeat(term: Int, leaderId: String) extends RaftMessage
case class LogEntry(term: Int, index: Int, command: String)
case class AppendEntries(term: Int, leaderId: String, prevLogIndex: Int, prevLogTerm: Int, entries: List[LogEntry], leaderCommit: Int, replyTo: ActorRef[AppendEntriesResponse] ) extends RaftMessage
case class SubmitCommand(command: String) extends RaftMessage
case class AppendEntriesResponse(term: Int, success: Boolean, matchIndex: Int) extends RaftMessage
case class ClientCommand(command: String, replyTo: ActorRef[ClientResponse]) extends RaftMessage
case class ClientResponse(success: Boolean, leaderId: Option[String]) extends RaftMessage
case class GetLogState(replyTo: ActorRef[LogStateResponse]) extends RaftMessage
case class LogStateResponse(nodeId: String,log: List[LogEntry],commitIndex: Int,lastApplied: Int,currentTerm: Int,state: String  ) extends RaftMessage
case class ShowLogs() extends RaftMessage