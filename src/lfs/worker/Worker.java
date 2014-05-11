package lfs.worker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import lfs.common.Conf;
import lfs.common.Constants;
import lfs.common.UnderFileSystem;
import lfs.master.MasterClient;
import lfs.thrift.NetAddress;
import lfs.thrift.WorkerService;
import lfs.thrift.WorkerService.Iface;
import lfs.utils.LFSUtils;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;




/**
 * worker主进程
 * @author paomian
 *
 */
public class Worker {
	private static final Logger LOG = Logger.getLogger(Worker.class);
	
	private InetSocketAddress masterAddr;
	private InetSocketAddress workerAddr;
	
	private DataServer dataServer;
	private Thread dataServerThread;	//	文件数据传输服务
	private Timer reportTimer;		//	工作节点状态报告服务
	
	private WorkerServiceHandler handler;
	private WorkerService.Processor<Iface> processor;
	private TServer RPCServer;
	
	private int selectorThreads = 2;
	private int acceptQueueSizePerThreads = 4;
	private int workerThreads = 5;
	
	private WorkerInfo workerInfo;	
	private Journal mJournal;
	private long volumeId;
	
	private boolean isStarted = false;
	
	private UnderFileSystem mainUFS;
	/**
	 * 
	 * @param masterAddr	
	 * @param workerAddr	本地数据服务线程地址
	 * @throws IOException 
	 */
	public Worker(InetSocketAddress masterAddr, InetSocketAddress workerAddr) {
		this.masterAddr = masterAddr;
		this.workerAddr = workerAddr;
	}
	
	public void register() throws TException {
		MasterClient client = new MasterClient();
		client.connect(masterAddr);
		volumeId = client.registerWorker(new NetAddress(workerAddr.getHostString(),
				workerAddr.getPort()));
		client.close();
		if (volumeId == -1) {
			LOG.error("重复注册,程序退出");
			System.exit(-1);
		}
		LOG.info("register successfully. alloctated volume id is：" + volumeId);
	}
	public void init() throws IOException, TTransportException {
		mainUFS = UnderFileSystem.get(Conf.getConfig().lfsDataDir);
		String journalFolder = Conf.getConfig().lfsDataDir;
		if (!journalFolder.endsWith("/")) {
			journalFolder += "/";
		}
		journalFolder += volumeId + "/";
//		mJournal = new Journal(journalFolder, "image.data", "log.data");
		mJournal = new Journal(mainUFS, journalFolder, "image.data", "log.data");
		workerInfo = new WorkerInfo((short)volumeId, mJournal, mainUFS);
		dataServer = new DataServer(workerAddr, workerInfo);
		dataServerThread = new Thread(dataServer);
		reportTimer = new Timer();
//		megerThread = new Thread(new BlockMerge(workerInfo));
//		handleCloseThread = new Thread(new HandleClose(this));
		
		handler = new WorkerServiceHandler(workerInfo);
		processor = new WorkerService.Processor<WorkerService.Iface>(handler);
		RPCServer = new TThreadedSelectorServer(new TThreadedSelectorServer.Args(
				new TNonblockingServerSocket(workerAddr)).processor(processor)
				.selectorThreads(selectorThreads)
				.acceptQueueSizePerThread(acceptQueueSizePerThreads)
				.workerThreads(workerThreads));
		
	}
	public void start() {
//		handleCloseThread.start();
		dataServerThread.start();
		LOG.info("starting data transmission server@" + workerAddr);
		reportTimer.schedule(new InfoReporter( masterAddr, workerAddr, workerInfo), 1000, 5000);
		LOG.info(String.format("starting info report server. delay time is %ds, interval time is %ds.", 1, 5));
//		megerThread.start();
//		LOG.info("启动文件合并服务线程。");
		
		RPCServer.serve();
		LOG.info("starting worker rpc server.");
		
		isStarted = true;
	}
	public void stop() throws IOException {
		if (isStarted) {
			reportTimer.cancel();
			LOG.info("状态汇报线程关闭.");
			workerInfo.flush();
			dataServer.close();
			LOG.info("数据传输线程关闭.");
			isStarted = false;
		}
	}

	public static void main(String[] args) throws TException, IOException {
		LFSUtils.initLog4j();
		if (args.length != 1) {
			LOG.error("usage: worker ip");
			System.exit(-1);
		}
		InetSocketAddress maddr = new InetSocketAddress(Conf.getConfig().masterHost, Conf.getConfig().masterPort);
		InetSocketAddress waddr = new InetSocketAddress(args[0], 9998);
		Worker worker = new Worker(maddr, waddr);
		worker.register();
		worker.init();
		worker.start();
	}
}
