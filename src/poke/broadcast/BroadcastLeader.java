package poke.broadcast;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.broadcast.management.BroadcastHandler;

public class BroadcastLeader extends Thread {
	protected static Logger logger = LoggerFactory
			.getLogger("broadcast leader");
	private final int publicPort;
	private final int broadcastPort = 8080;
	private volatile boolean forever = true;

	public BroadcastLeader(int port) {
		// TODO Auto-generated constructor stub
		this.publicPort = port;
	}

	public void run() {
		EventLoopGroup group = new NioEventLoopGroup();
		while (forever) {
			try {
				Thread.sleep(5000);
				Bootstrap b = new Bootstrap();
				b.group(group);
				b.channel(NioDatagramChannel.class);
				b.option(ChannelOption.SO_BROADCAST, true);
				b.handler(new BroadcastHandler());

				Channel channel = b.bind(0).sync().channel();

				logger.info("preparing to broadcast the public port");

				channel.writeAndFlush(
						new DatagramPacket(Unpooled.copiedBuffer(
								"public port to send job requests :"
										+ publicPort, CharsetUtil.UTF_8),
								new InetSocketAddress("255.255.255.255",
										broadcastPort))).sync();

				logger.info("started broadcasting");

			} catch (Exception ex) {
				ex.printStackTrace();
				group.shutdownGracefully();
			}
		}

	}

	public void terminateBroadcast() {
		forever = false;

	}

}