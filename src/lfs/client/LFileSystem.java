package lfs.client;

import java.net.InetSocketAddress;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import lfs.common.Conf;
import lfs.master.MasterClient;
import lfs.thrift.ClientFileInfo;
import lfs.thrift.NetAddress;
import lfs.thrift.RefuseServiceException;
import lfs.worker.WorkerClient;

public class LFileSystem {
	private MasterClient masterClient = new MasterClient();
	
	private static LFileSystem lfs = new LFileSystem();
	private LFileSystem() {
	}
	
	public static LFileSystem get() throws TTransportException {
		if (!lfs.masterClient.isConned()) {
			InetSocketAddress master =  new InetSocketAddress(Conf.getConfig().masterHost, Conf.getConfig().masterPort);
			lfs.masterClient.connect(master);
		}
		return lfs;
	}
	
	public void close() {
		masterClient.close();
	}
	
	public boolean exist(long id) throws RefuseServiceException, TException {
		short vid = (short)(id >>> 48);
		NetAddress addr = masterClient.getVolumeToWorkerAddress(vid);
		WorkerClient wc = new WorkerClient();
		wc.connect(new InetSocketAddress(addr.host, addr.port));
		boolean ret = wc.exist(id);
		wc.close();
		return ret;
	}
	
	public LFile open(long id) throws RefuseServiceException, TException {
		short vid = (short)(id >>> 48);
		NetAddress addr = masterClient.getVolumeToWorkerAddress(vid);
		WorkerClient wc = new WorkerClient();
		wc.connect(new InetSocketAddress(addr.host, addr.port));
		boolean isExist = wc.exist(id);
		ClientFileInfo info = wc.getCLientFileInfo(id); 
		LFile file = new LFile(isExist, info);
		return file;
	}
	
	public int getFileLength(long id) throws RefuseServiceException, TException {
		short vid = (short)(id >>> 48);
		NetAddress addr = masterClient.getVolumeToWorkerAddress(vid);
		WorkerClient wc = new WorkerClient();
		wc.connect(new InetSocketAddress(addr.host, addr.port));
		ClientFileInfo info = wc.getCLientFileInfo(id);
		wc.close();
		return info.length;
	}
	
	public boolean isCached(long id) throws RefuseServiceException, TException {
		short vid = (short)(id >>> 48);
		NetAddress addr = masterClient.getVolumeToWorkerAddress(vid);
		WorkerClient wc = new WorkerClient();
		wc.connect(new InetSocketAddress(addr.host, addr.port));
		ClientFileInfo info = wc.getCLientFileInfo(id);
		wc.close();
		return info.isCached;
	}
	
}
