package raft
import akka.actor.typed.ActorRef
import raft.RaftMessage

case object ElectionTimeout extends RaftMessage
case class UpdatePeers(peers: Map[String, ActorRef[RaftMessage]]) extends RaftMessage
case object ShowStatus extends RaftMessage
case class CrashNode(nodeId: String) extends RaftMessage
case class JoinCluster(nodeId: String) extends RaftMessage
case class LeaveCluster(nodeId: String) extends RaftMessage