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
