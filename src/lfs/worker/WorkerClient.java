package lfs.worker;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import lfs.thrift.ClientFileInfo;
import lfs.thrift.RefuseServiceException;
import lfs.thrift.WorkerService;

public class WorkerClient {
	private WorkerService.Client serverRPC;
	private TProtocol protocol;
	private boolean isConned = false;
	
	public void connect(InetSocketAddress addr) throws TTransportException {
		protocol = new TBinaryProtocol(new TFramedTransport(
	              new TSocket(addr.getHostName(), addr.getPort())));
		serverRPC = new WorkerService.Client(protocol);
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
	
	public boolean exist(long id) throws RefuseServiceException, TException {
		return serverRPC.exist(id);
	}
	
	public ClientFileInfo getCLientFileInfo(long id) throws RefuseServiceException, TException {
		return serverRPC.getClientFileInfo(id);
	}
	
	public List<Integer> listBlockByVid(short volumeId) throws RefuseServiceException, TException {
		return serverRPC.listBlockByVid(volumeId);
	}
	
	public List<Short> listFileByBid(short volumeId, int blockId) throws RefuseServiceException, TException {
		return serverRPC.listFileByBid(volumeId, blockId);
	}
	
	public void delete(long id) throws TException {
		throw new TException("delete() is not implemented.");
	}
}
