package poke.server.management.managers;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.LeaderElection;
import eye.Comm.Management;
import eye.Comm.LeaderElection.VoteAction;
import poke.server.management.ManagementInitializer;

public class LeaderElectionData {
	protected static Logger logger = LoggerFactory.getLogger("management");
	private String nodeId;
	private String host;
	private int port;
	public int active;

	public LeaderElectionData(String nodeId, String host, int port) {
		this.nodeId = nodeId;
		this.host = host;
		this.port = port;

	}

	/**
	 * An assigned unique key (node ID) to the remote connection
	 * 
	 * @return
	 */
	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	/**
	 * The host to connect to
	 * 
	 * @return
	 */
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * The port the remote connection is listening to
	 * 
	 * @return
	 */
	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getActive() {
		return active;
	}

	public void setActive(int active) {
		this.active = active;
	}

}
