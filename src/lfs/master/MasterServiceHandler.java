package lfs.master;

import java.util.List;

import org.apache.thrift.TException;

import lfs.common.Constants;
import lfs.thrift.MasterService;
import lfs.thrift.NetAddress;
import lfs.thrift.WorkerData;

/**
 * 通信接口的具体实现
 * @author paomian
 *
 */
public class MasterServiceHandler implements MasterService.Iface{
	private MasterInfo masterInfo;
	
	public MasterServiceHandler(MasterInfo masterinfo) {
		this.masterInfo = masterinfo;
	}

	@Override
	public NetAddress getVolumeToWorkerAddress(short volumeId)
			throws TException {
		return masterInfo.getVolume2WorkerAddress(volumeId);
	}

	@Override
	public NetAddress getWriteableWorkerAddress() throws TException {
		// TODO Auto-generated method stub
		return masterInfo.getWriteableWorkerAddress();
	}

	@Override
	public NetAddress getWriteableWorkerAddressByHash(String fname)
			throws TException {
		// TODO Auto-generated method stub
		return masterInfo.getWriteableWorkerAddressByHash(fname);
	}

	@Override
	public int getWorkerNumber() throws TException {
		// TODO Auto-generated method stub
		return masterInfo.getWorkerNumber();
	}

	@Override
	public WorkerData getWorkerData(short volumeId) throws TException {
		// TODO Auto-generated method stub
		return masterInfo.getWorkerData(volumeId);
	}

	@Override
	public List<Short> getAllocedVolumeIdList() throws TException {
		// TODO Auto-generated method stub
		return masterInfo.getAllocedVolumeIdList();
	}

	@Override
	public long registerWorker(NetAddress addr) throws TException {
		// TODO Auto-generated method stub
		return masterInfo.registerWorker(addr);
	}

	@Override
	public byte updateWorkerInfo(NetAddress addr, WorkerData data)
			throws TException {
		// TODO Auto-generated method stub
		return masterInfo.updateWorkerInfo(addr, data);
	}

	@Override
	public NetAddress getWriteableWorkerAddressByCAddr(NetAddress caddr)
			throws TException {
		// TODO Auto-generated method stub
		return masterInfo.getWriteableWorkerAddressByCAddr(caddr);
	}



	



}
