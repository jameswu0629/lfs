package lfs.master;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import lfs.thrift.MasterService;
import lfs.thrift.NetAddress;
import lfs.thrift.WorkerData;

public class MasterClient {
	private MasterService.Client client;
	private TProtocol protocol;
	private boolean isConned = false;
	public void connect(InetSocketAddress addr) throws TTransportException {
		protocol = new TBinaryProtocol(new TFramedTransport(
	              new TSocket(addr.getHostName(), addr.getPort())));
		client = new MasterService.Client(protocol);
		protocol.getTransport().open();
		isConned = true;
	}
	
	public void close() {
		protocol.getTransport().close();
		isConned = false;
	}
	
	public boolean isConned() {
		return isConned;
	}
	
	/**
	 * 
	 * @param volumeId
	 * @return address or null
	 * @throws TException
	 */
	public NetAddress getVolumeToWorkerAddress(short volumeId)
			throws TException {
		return client.getVolumeToWorkerAddress(volumeId);
	}

	public NetAddress getWriteableWorkerAddress() throws TException {
		// TODO Auto-generated method stub
		return client.getWriteableWorkerAddress();
	}

	public NetAddress getWriteableWorkerAddressByHash(String fname)
			throws TException {
		// TODO Auto-generated method stub
		return client.getWriteableWorkerAddressByHash(fname);
	}

	public int getWorkerNumber() throws TException {
		// TODO Auto-generated method stub
		return client.getWorkerNumber();
	}

	public WorkerData getWorkerData(short volumeId) throws TException {
		// TODO Auto-generated method stub
		return client.getWorkerData(volumeId);
	}

	public List<Short> getAllocedVolumeIdList() throws TException {
		// TODO Auto-generated method stub
		return client.getAllocedVolumeIdList();
	}

	public long registerWorker(NetAddress addr) throws TException {
		// TODO Auto-generated method stub
		return client.registerWorker(addr);
	}

	public byte updateWorkerInfo(NetAddress addr, WorkerData data)
			throws TException {
		// TODO Auto-generated method stub
		return client.updateWorkerInfo(addr, data);
	}

	public NetAddress getWriteableWorkerAddressByCAddr(NetAddress caddr)
			throws TException {
		// TODO Auto-generated method stub
		return client.getWriteableWorkerAddressByCAddr(caddr);
	}

}
