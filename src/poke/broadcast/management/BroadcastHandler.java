package poke.broadcast.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;

public class BroadcastHandler extends SimpleChannelInboundHandler<DatagramPacket> {
	protected static Logger logger = LoggerFactory.getLogger("broadcast handler");

	@Override
	protected void channelRead0(ChannelHandlerContext arg0, DatagramPacket arg1)
			throws Exception {
		// TODO Auto-generated method stub
		
		logger.info("inside handler");
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		//logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}
