package lfs.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import lfs.common.Conf;
import lfs.common.Constants;
import lfs.master.MasterClient;
import lfs.thrift.NetAddress;
import lfs.worker.DataMessage;

public class LFSClient {
	private static final Logger LOG = Logger.getLogger(LFSClient.class);
	private MasterClient masterClient;
	private LFSClient() {
		masterClient = new MasterClient();
	}
	public static LFSClient get() throws TTransportException {
		LFSClient lfsclient = new LFSClient();
		InetSocketAddress master =  new InetSocketAddress(Conf.getConfig().masterHost, Conf.getConfig().masterPort);
		lfsclient.masterClient.connect(master);
		return lfsclient;
	}
	public void close() {
		masterClient.close();
	}
	
	/**
	 * write
	 * @param buf
	 * @return
	 * @throws IOException
	 * @throws TException
	 */
	public long write(byte[] buf) throws IOException, TException {
		NetAddress addr = masterClient.getWriteableWorkerAddress();
		SocketChannel channel = SocketChannel.open();
		channel.connect(new InetSocketAddress(addr.host, addr.port));
		DataMessage msg = DataMessage.createWriteRequestMessage(buf);
		while(!msg.send(channel)){
		}
		LOG.debug(msg + " send.");
		DataMessage respmsg = new DataMessage();
		while (!respmsg.isReady()) {
			respmsg.recv(channel);
		}
		LOG.debug(respmsg + " read.");
		channel.close();
		if (respmsg.errorCode == 1)
			throw new IOException("errorCode.");
		return (long)respmsg.volumeid << 48 | (long)respmsg.blockid << 16 | (long)respmsg.infileid ;
	}
	
	/**
	 * read
	 * @param id
	 * @return
	 * @throws IOException
	 * @throws TException
	 */
	public ByteBuffer read (long id) throws IOException, TException {
		return read(id, 0, 0);
	}
	
	/**
	 * 
	 * @param id
	 * @param offset
	 * @param len
	 * @return
	 * @throws IOException
	 * @throws TException
	 */
	public ByteBuffer read(long id, int offset, int len) throws IOException, TException {
		short fid = (short)(id & 0xFFFF);
		int bid = (int)(id >>> 16 & 0xFFFFFFFF);
		short vid = (short)(id >>> 48);
		
		NetAddress addr = masterClient.getVolumeToWorkerAddress(vid);
		if (addr.equals(Constants.EMPTY_ADDR)) {
			throw new TException("worker节点未启动。卷编号：" + vid);
		}
		SocketChannel channel = SocketChannel.open();
		channel.connect(new InetSocketAddress(addr.host, addr.port));

		DataMessage msg = DataMessage.createReadRequestMessage(vid, bid, fid, offset, len);
		while(!msg.send(channel)){
		}
		LOG.debug(msg + " send.");
		DataMessage respmsg = new DataMessage();
		while (!respmsg.isReady()) {
			int nRecv = respmsg.recv(channel);
			LOG.debug("read bytes number: " + nRecv);
		}
		LOG.debug(respmsg + " read.");
		channel.close();
		if (respmsg.errorCode != 0) {
			throw new IOException("读异常.");
		}
		return respmsg.data;
	}
	
	public FSDataInputStream getFileInputStream(long id) {
		return null;
	}
	
	public int getWorkerNum() throws TException {
		return masterClient.getWorkerNumber();
	}
	
	public List<Short> getVolumeIdList() throws TException {
		return masterClient.getAllocedVolumeIdList();
	}
	
//	public static void main(String[] args) throws TException {
//		LFSClient client = LFSClient.get();
//		System.out.println(client.getWorkerNum());
//		client.client.getVolumeToWorkerAddress((short)1);
//	}
}
