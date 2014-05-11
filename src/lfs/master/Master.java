package lfs.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import lfs.common.Conf;
import lfs.thrift.MasterService;
import lfs.thrift.MasterService.Iface;
import lfs.utils.LFSUtils;

/**
 * master 主进程
 * @author paomian
 *
 */
public class Master {
	private static final Logger LOG = Logger.getLogger(Master.class);
	private MasterServiceHandler handler;
	private MasterService.Processor<Iface> processor;
	private MasterInfo masterInfo;
	private InetSocketAddress masterAddr;
	private TServer RPCServer;
	
	private int selectorThreads = 2;
	private int acceptQueueSizePerThreads = 4;
	private int workerThreads = 5;
	
	private boolean isStarted = false; 
	
	public Master(InetSocketAddress addr) {
		this.masterAddr = addr;
	}
	private void init() throws TTransportException, IOException {
		this.masterInfo = new MasterInfo();
		this.handler = new MasterServiceHandler(this.masterInfo);
		this.processor = new MasterService.Processor<Iface>(this.handler);
		RPCServer = new TThreadedSelectorServer(new TThreadedSelectorServer.Args(
				new TNonblockingServerSocket(masterAddr)).processor(processor)
				.selectorThreads(selectorThreads)
				.acceptQueueSizePerThread(acceptQueueSizePerThreads)
				.workerThreads(workerThreads));
	}
	
	public void start() {
		try {
			init();
		} catch (TTransportException | IOException e) {
			LOG.error(e.getMessage());
			System.exit(-1);
		}
		LOG.info("The master server started @ " + masterAddr);
		RPCServer.serve();
		LOG.info("The master server ended @ " + masterAddr);
		isStarted = true;
	}

	public void stop() {
//		if (isStarted) {
			masterInfo.stop();
			RPCServer.stop();
//		}
	}
	
	public class HeartBeatChecker extends TimerTask {
		
		@Override
		public void run() {
			
		}
		
	}
	public static void main(String[] args) throws IOException {
		LFSUtils.initLog4j();
		InetSocketAddress addr = new InetSocketAddress(Conf.getConfig().masterHost, Conf.getConfig().masterPort);
		final Master master = new Master(addr);
		master.start();
	}
}
