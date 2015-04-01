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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.resources.JobResource;
import poke.server.ServerInitializer;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.ManagementInitializer;
import poke.server.management.ManagementQueue;
import poke.server.management.ManagementQueue.ManagementQueueEntry;
import poke.server.queue.ChannelQueue;
import poke.server.queue.QueueFactory;
import poke.server.resources.ResourceUtil;
import eye.Comm.Header;
import eye.Comm.JobBid;
import eye.Comm.JobDesc;
import eye.Comm.JobOperation;
import eye.Comm.JobProposal;
import eye.Comm.JobStatus;
import eye.Comm.Management;
import eye.Comm.Payload;
import eye.Comm.PokeStatus;
import eye.Comm.Request;
import eye.Comm.JobDesc.JobCode;
import eye.Comm.JobOperation.JobAction;

/**
 * The job manager class is used by the system to assess and vote on a job. This
 * is used to ensure leveling of the servers take into account the diversity of
 * the network.
 * 
 * @author gash
 * 
 */
public class JobManager {
	//dsdsd
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<JobManager> instance = new AtomicReference<JobManager>();
	private static final String competition = "competition";

	private String nodeId;
	private ServerConf configFile;
	LinkedBlockingDeque<JobBid> bidQueue;
	private static Map<String, JobBid> bidMap;
	private static final String getMoreCourses = "getmorecourses";

	private static final String listCourses = "listcourses";
	private Map<String, Channel> channelMap = new HashMap<String, Channel>();

	public void addToChannelMap(String jobId, Channel ch) {
		channelMap.put(jobId, ch);
	}

	public static JobManager getInstance(String id, ServerConf configFile) {
		instance.compareAndSet(null, new JobManager(id, configFile));
		return instance.get();
	}

	public static JobManager getInstance() {
		return instance.get();
	}

	public JobManager(String nodeId, ServerConf configFile) {
		this.nodeId = nodeId;
		this.configFile = configFile;
		this.bidMap = new HashMap<String, JobBid>();
		bidQueue = new LinkedBlockingDeque<JobBid>();
	}

	/**
	 * a new job proposal has been sent out that I need to evaluate if I can run
	 * it
	 * 
	 * @param req
	 *            The proposal
	 */
	public void processRequest(JobProposal req) {

		String leaderId = ElectionManager.getInstance().getLeader();

		logger.info("\n**********\nRECEIVED NEW JOB PROPOSAL ID:"
				+ req.getJobId() + "\n**********");
		Management.Builder mb = Management.newBuilder();
		JobBid.Builder jb = JobBid.newBuilder();
		jb.setJobId(req.getJobId());
		jb.setNameSpace(req.getNameSpace());
		jb.setOwnerId(Long.parseLong(nodeId));
		int bid = (int) Math.floor(Math.random() + 0.5);
		jb.setBid(bid);

		mb.setJobBid(jb.build());

		Management jobBid = mb.build();

		if (req.getNameSpace().equals(getMoreCourses)
				|| req.getNameSpace().equals(competition)) {

			Channel ch = channelMap.get(req.getJobId());
			ch.writeAndFlush(jobBid);
			channelMap.remove(req.getJobId());

			logger.info("\n**********sent a job bid with Job Id: **********"
					+ req.getJobId());

		} else {

			NodeDesc leaderNode = configFile.getNearest().getNearestNodes()
					.get(leaderId);

			InetSocketAddress sa = new InetSocketAddress(leaderNode.getHost(),
					leaderNode.getMgmtPort());

			Channel ch = connectToManagement(sa);
			ch.writeAndFlush(jobBid);
			logger.info("\n**********sent a job bid with Job Id: **********"
					+ req.getJobId());

		}

	}

	public Channel connectToManagement(InetSocketAddress sa) {
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

			channelFuture = b.connect(sa);
			channelFuture.awaitUninterruptibly(5000l);

		} catch (Exception ex) {
			logger.debug("failed to initialize the election connection");

		}

		if (channelFuture != null && channelFuture.isDone()
				&& channelFuture.isSuccess())
			return channelFuture.channel();
		else
			throw new RuntimeException(
					"Not able to establish connection to server");
	}

	/**
	 * a job bid for my job
	 * 
	 * @param req
	 *            The bid
	 */
	public void processRequest(JobBid req) {
		logger.info("\n**********\nRECEIVED NEW JOB BID" + "\n\n**********");
		logger.info("****************Bid value********" + req.getBid());

		String leaderId = ElectionManager.getInstance().getLeader();
		if (leaderId != null && leaderId.equalsIgnoreCase(nodeId)) {
			if (bidMap.containsKey(req.getJobId())) {
				return;
			}
			if (req.getBid() == 1) {
				bidQueue.add(req);
				bidMap.put(req.getJobId(), req);
			}

			if (req.getBid() == 1) {
				Map<String, Request> requestMap = JobResource.getRequestMap();
				Request jobOperation = requestMap.get(req.getJobId());
				String toNodeId = req.getOwnerId() + "";

				if (jobOperation.getBody().getJobOp().getData().getNameSpace()
						.equals(getMoreCourses)) {

					Request.Builder rb = Request.newBuilder(jobOperation);
					Header.Builder hbldr = rb.getHeaderBuilder();
					hbldr.setToNode(toNodeId);
					hbldr.setRoutingId(Header.Routing.JOBS);
					rb.setHeader(hbldr.build());

					Payload.Builder pb = rb.getBodyBuilder();

					JobOperation.Builder jobOpBuilder = pb.getJobOpBuilder();
					jobOpBuilder.setAction(JobAction.ADDJOB);

					JobDesc.Builder jDescBuilder = JobDesc.newBuilder();
					jDescBuilder.setJobId(req.getJobId());
					jDescBuilder.setOwnerId(req.getOwnerId());

					jDescBuilder.setNameSpace(listCourses);
					jDescBuilder.setStatus(JobCode.JOBQUEUED);

					jobOpBuilder.setData(jDescBuilder.build());

					pb.setJobOp(jobOpBuilder.build());
					rb.setBody(pb.build());
					Request jobDispatched = rb.build();

					Channel ch = channelMap.get(req.getJobId());

					InetSocketAddress inetSa = (InetSocketAddress) ch
							.remoteAddress();
					String hostname = null;
					if (inetSa != null)
						hostname = inetSa.getAddress().getHostAddress();
					channelMap.remove(req.getJobId());
					logger.info("\n****************HOSTNAME RESOLVED FROM CHANNEL: "
							+ hostname + "********");
					int port = 5573;
					logger.info("****************Job Request being dispatched to leader********");

					InetSocketAddress sa = new InetSocketAddress(hostname, port);
					Channel out = connectToPublic(sa);
					ChannelQueue queue = QueueFactory.getInstance(out);
					queue.enqueueResponse(jobDispatched, out);

				} else if (jobOperation.getBody().getJobOp().getData()
						.getNameSpace().equals(competition)) {

					// reply success
					Request.Builder rb = Request.newBuilder();
					// metadata
					rb.setHeader(ResourceUtil.buildHeader(jobOperation
							.getHeader().getRoutingId(), PokeStatus.SUCCESS,
							"competition request processed", jobOperation
									.getHeader().getOriginator(), jobOperation
									.getHeader().getTag(), leaderId));

					// payload
					Payload.Builder pb = Payload.newBuilder();
					JobStatus.Builder jb = JobStatus.newBuilder();
					jb.setStatus(PokeStatus.SUCCESS);
					jb.setJobId(jobOperation.getBody().getJobOp().getJobId());
					jb.setJobState(JobDesc.JobCode.JOBRECEIVED);
					pb.setJobStatus(jb.build());

					rb.setBody(pb.build());

					Request reply = rb.build();

					Channel ch = JobResource.getChMap().get(
							jobOperation.getBody().getJobOp().getJobId());

					ch.writeAndFlush(reply);

				} else {

					Request.Builder rb = Request.newBuilder(jobOperation);
					Header.Builder hbldr = rb.getHeaderBuilder();
					hbldr.setToNode(toNodeId);
					hbldr.setRoutingId(Header.Routing.JOBS);
					rb.setHeader(hbldr.build());
					Request jobDispatched = rb.build();

					NodeDesc slaveNode = configFile.getNearest().getNode(
							toNodeId);

					InetSocketAddress sa = new InetSocketAddress(
							slaveNode.getHost(), slaveNode.getPort());

					Channel ch = connectToPublic(sa);

					ChannelQueue queue = QueueFactory.getInstance(ch);
					logger.info("****************Job Request being dispatched to slave node: ********"
							+ toNodeId);
					queue.enqueueResponse(jobDispatched, ch);

				}

			}

		}

	}

	public Channel connectToPublic(InetSocketAddress sa) {
		// Start the connection attempt.
		ChannelFuture channelFuture = null;
		EventLoopGroup group = new NioEventLoopGroup();

		try {
			ServerInitializer initializer = new ServerInitializer(false);
			Bootstrap b = new Bootstrap();

			b.group(group).channel(NioSocketChannel.class).handler(initializer);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			channelFuture = b.connect(sa);
			channelFuture.awaitUninterruptibly(5000l);

		} catch (Exception ex) {
			logger.debug("failed to initialize the election connection");

		}

		if (channelFuture != null && channelFuture.isDone()
				&& channelFuture.isSuccess())
			return channelFuture.channel();
		else
			throw new RuntimeException(
					"Not able to establish connection to server");
	}

}
