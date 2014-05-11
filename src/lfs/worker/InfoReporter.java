package lfs.worker;

import java.net.InetSocketAddress;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import lfs.common.Constants;
import lfs.master.MasterClient;
import lfs.thrift.NetAddress;
import lfs.utils.LFSUtils;

public class InfoReporter extends TimerTask {
	private static final Logger LOG = Logger.getLogger(InfoReporter.class);
	private MasterClient client = new MasterClient();
	private InetSocketAddress masterAddr;
	private NetAddress workerAddr;
	private WorkerInfo workerinfo;
//	private BlockingQueue<Long> cmdQueue;
	
	public InfoReporter(InetSocketAddress masterAddr, InetSocketAddress workerAddr,
			WorkerInfo workerinfo/*, BlockingQueue<Long> cmdQueue*/) {
		this.masterAddr = masterAddr;
		this.workerAddr = new NetAddress(workerAddr.getHostString(), workerAddr.getPort());
		this.workerinfo = workerinfo;
//		this.cmdQueue = cmdQueue;
	}
	@Override
	public void run() {
		try {
			client.connect(masterAddr);
			client.updateWorkerInfo(workerAddr, null);
		} catch (TException e) {
			LOG.error(e);
//			cmdQueue.add(Constants.CMD_CLOSE);
		}
		client.close();
	}

}
