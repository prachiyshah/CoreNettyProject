/*
 * copyright 2014, gash 
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.management.managers;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.monitor.HeartMonitor;
import poke.broadcast.*;
import poke.server.management.managers.LeaderElectionData;
import poke.server.management.ManagementInitializer;
import eye.Comm.LeaderElection;
import eye.Comm.LeaderElection.VoteAction;
import eye.Comm.Management;

/**
 * The election manager is used to determine leadership within the network.
 * 
 * @author gash
 * 
 */
public class ElectionManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<ElectionManager> instance = new AtomicReference<ElectionManager>();
	private static ConcurrentLinkedQueue<LeaderElectionData> neighbours = new ConcurrentLinkedQueue<LeaderElectionData>();
	private static BroadcastLeader broadcast;

	private String nodeId;

	private static String leader = null;
	private boolean participant = false;

	private String previousLeader;
	private boolean possibleLeader = true;

	private String destHost;
	private int destPort;

	private Channel channel;

	private boolean coordinatorMsg;
	private boolean electionMsg;
	private boolean electionMsgSend;

	private boolean okMsgRecieved;
	int msgType = 1;
	private String senderId;

	/** @brief the number of votes this server can cast */
	private int votes = 1;
	private int myPublicPort;

	public ConcurrentLinkedQueue<LeaderElectionData> getNeighbours() {
		return neighbours;
	}

	public int getMyPublicPort() {
		return myPublicPort;
	}

	public void setMyPublicPort(int myPublicPort) {
		this.myPublicPort = myPublicPort;
	}

	public static ElectionManager getInstance(String id, int votes) {
		instance.compareAndSet(null, new ElectionManager(id, votes));
		return instance.get();
	}

	public static ElectionManager getInstance() {
		return instance.get();
	}

	public String getLeader() {
		return leader;
	}

	public void setLeader(String leaderValue) {
		leader = leaderValue;

	}

	public boolean isParticipant() {
		return participant;
	}

	public void setParticipant(boolean participant) {
		this.participant = participant;
	}

	public boolean isElectionMsg() {
		return electionMsg;
	}

	public void setElectionMsg(boolean electionMsg) {
		this.electionMsg = electionMsg;
	}

	public boolean isOkMsgRecieved() {
		return okMsgRecieved;
	}

	public void setOkMsgRecieved(boolean okMsgRecieved) {
		this.okMsgRecieved = okMsgRecieved;
	}

	/**
	 * initialize the manager for this server
	 * 
	 * @param nodeId
	 *            The server's (this) ID
	 */
	protected ElectionManager(String nodeId, int votes) {
		this.nodeId = nodeId;

		if (votes >= 0)
			this.votes = votes;

	}

	public void addConnectToThisNode(String nodeId, String host, int mgmtport,
			int status) {

		LeaderElectionData ld = new LeaderElectionData(nodeId, host, mgmtport);
		ld.setActive(status);
		neighbours.add(ld);

	}

	public String getPreviousLeader() {
		return previousLeader;
	}

	public void setPreviousLeader(String previousLeader) {
		this.previousLeader = previousLeader;
	}

	private Management generateLE(VoteAction vote, String nodeId) {
		LeaderElection.Builder electionBuilder = LeaderElection.newBuilder();
		electionBuilder.setNodeId(nodeId);
		electionBuilder.setBallotId("0");
		electionBuilder.setDesc("election message");
		electionBuilder.setVote(vote);
		LeaderElection electionMsg = electionBuilder.build();

		Management.Builder mBuilder = Management.newBuilder();
		mBuilder.setElection(electionMsg);
		Management msg = mBuilder.build();

		return msg;
	}

	public Channel connect(String host, int port) {
		// Start the connection attempt.
		ChannelFuture channelFuture = null;
		EventLoopGroup group = new NioEventLoopGroup();

		try {
			ManagementInitializer mi = new ManagementInitializer(false);
			Bootstrap b = new Bootstrap();

			b.group(group).channel(NioSocketChannel.class).handler(mi);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			InetSocketAddress destination = new InetSocketAddress(host, port);

			channelFuture = b.connect(destination);
			channelFuture.awaitUninterruptibly(5000l);

		} catch (Exception ex) {
			logger.debug("failed to initialize the election connection" + host);

		}

		if (channelFuture != null && channelFuture.isDone()
				&& channelFuture.isSuccess())
			return channelFuture.channel();
		else
			throw new RuntimeException(
					"Not able to establish connection to server");
	}

	public boolean checkIamGreater() {

		int myId = Integer.parseInt(nodeId);
		for (LeaderElectionData ld : neighbours) {
			int neighbourId = Integer.parseInt(ld.getNodeId());
			if (myId > neighbourId && ld.getActive() == 1) {
				possibleLeader = true;
				break;
			} else if (myId < neighbourId && ld.getActive() == 1) {
				possibleLeader = false;
				break;
			}
		}
		return possibleLeader;
	}

	private void send(Management msg) {
		int myId = Integer.parseInt(nodeId);
		Channel channel;
		try {

			switch (msgType) {
			case 1:
				participant = true;
				for (LeaderElectionData ld : neighbours) {
					int neighbourId = Integer.parseInt(ld.getNodeId());
					logger.info("my Id---" + myId + "neighbour" + neighbourId);
					if (neighbourId > myId && (ld.getActive() == 1)) {
						electionMsgSend = true;
						channel = connect(ld.getHost(), ld.getPort());
						channel.writeAndFlush(msg);
						participant = true;
						logger.info("Election message (" + nodeId
								+ ") sent to " + ld.getNodeId() + " at "
								+ ld.getPort());

					}
				}
				break;
			case 2:
				for (LeaderElectionData ld : neighbours) {
					if (senderId.equals(ld.getNodeId())) {
						channel = connect(ld.getHost(), ld.getPort());
						channel.writeAndFlush(msg);
						logger.info("message" + nodeId + " sent to "
								+ ld.getHost() + " at " + ld.getPort() + ""
								+ ld.getNodeId());

					}
				}
				participant = true;
				break;

			case 3:// co-rrdinator msg send to all lower ids
				for (LeaderElectionData ld : neighbours) {
					int neighbourId = Integer.parseInt(ld.getNodeId());

					if (neighbourId < myId && (ld.getActive() == 1)) {

						channel = connect(ld.getHost(), ld.getPort());
						channel.writeAndFlush(msg);
						participant = true;
						logger.info("Co-ordinator message (" + nodeId
								+ ") sent to " + ld.getHost() + " at "
								+ ld.getPort());

					}
				}

				break;
			default:
				logger.info("unknown message type");
				break;
			}

		} catch (Exception e) {
			e.printStackTrace();
			if (msgType == 1) {
				electionMsgSend = false;
				logger.error("Failed to send leader election message");
			} else if (msgType == 2) {
				logger.error("Failed to send ok message");
			} else {
				logger.error("Failed to send co-ordinator message");
			}
		}

	}

	public void initiateElection() {

		logger.info("starting Election manager");

		Management msg = null;

		if (leader == null && !participant) {
			msgType = 1;
			msg = generateLE(LeaderElection.VoteAction.ELECTION, nodeId);
			send(msg);
		}
		if ((!electionMsgSend) && leader == null) {
			HeartMonitor.leaderDown = false;
			msg = generateLE(LeaderElection.VoteAction.DECLAREWINNER, nodeId);
			participant = false;
			leader = nodeId;
			msgType = 3;
			send(msg);
		} else if (!okMsgRecieved && electionMsg) {
			logger.info("No ok or co-ordinator message recieved");
			// I am the leader
			HeartMonitor.leaderDown = false;
			msg = generateLE(LeaderElection.VoteAction.DECLAREWINNER, nodeId);
			participant = false;
			leader = nodeId;
			msgType = 3;
			send(msg);

		}

	}

	// retrieve the leader of the network
	public String whoIsTheLeader() {
		if (leader != null)
			return leader;
		else
			return null;

	}

	public void setChannel(ChannelFuture f) {
		channel = f.channel();
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public void processRequest(LeaderElection req) throws Exception {
		if (req == null)
			return;

		// logger.info("Received an election request..");
		Management msg = null;
		if (req.hasExpires()) {
			long ct = System.currentTimeMillis();
			if (ct > req.getExpires()) {
				// election is over
				return;
			}
		}

		if (req.getVote().getNumber() == VoteAction.ELECTION_VALUE) {
			// an election is declared!
			logger.info("Election declared!");
			senderId = req.getNodeId();
			if (leader == null && checkIamGreater()) {

				HeartMonitor.leaderDown = false;
				msg = generateLE(LeaderElection.VoteAction.DECLAREWINNER,
						nodeId);
				participant = false;
				leader = nodeId;
				msgType = 2;
				send(msg);

			} else if (leader != null && leader.equals(nodeId)) {
				logger.info("I am greater and i am the starting node");
				HeartMonitor.leaderDown = false;
				msg = generateLE(LeaderElection.VoteAction.DECLAREWINNER,
						nodeId);
				participant = false;
				msgType = 2;
				send(msg);

			} else {
				electionMsg = true;
				msg = generateLE(LeaderElection.VoteAction.ABSTAIN, nodeId);
				msgType = 2;
				send(msg);
			}
			participant = true;

		} else if (req.getVote().getNumber() == VoteAction.DECLAREVOID_VALUE) {
			// no one was elected, I am dropping into standby mode
			participant = false;
		} else if (req.getVote().getNumber() == VoteAction.DECLAREWINNER_VALUE) {
			// some node declared themself the leader
			HeartMonitor.leaderDown = false;
			okMsgRecieved = true;
			leader = req.getNodeId();
			participant = false;
			logger.info("Winner declared! leader is :" + leader);

		} else if (req.getVote().getNumber() == VoteAction.ABSTAIN_VALUE) {
			// I am not participating in the election
			okMsgRecieved = true;
			participant = false;
		}

		if (leader != null) {
			if (leader.equals(nodeId)) {
				logger.info("I am the leader " + leader);
				broadcast = new BroadcastLeader(myPublicPort);
				broadcast.start();
			} else if (broadcast != null) {
				broadcast.terminateBroadcast();
			}
		}
	}

}