package lfs.worker;

import java.util.List;

import org.apache.thrift.TException;

import lfs.thrift.ClientFileInfo;
import lfs.thrift.RefuseServiceException;
import lfs.thrift.WorkerService;

public class WorkerServiceHandler implements WorkerService.Iface {
	private WorkerInfo workerInfo;
	
	public WorkerServiceHandler(WorkerInfo workerInfo) {
		this.workerInfo = workerInfo;
	}
	
	@Override
	public boolean exist(long id) throws RefuseServiceException, TException {
		short fid = (short)(id & 0xFFFF);
		int bid = (int)(id >>> 16 & 0xFFFFFFFF);
		short vid = (short)(id >>> 48);
		
		if (vid != workerInfo.getVolumeid()) {
			throw new RefuseServiceException("The volume id is not matched.");
		}
		return workerInfo.existFile(bid, fid);
	}

	@Override
	public void delete_file(long id) throws RefuseServiceException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Integer> listBlockByVid(short volumeId)
			throws RefuseServiceException, TException {
		if (volumeId != workerInfo.getVolumeid()) {
			throw new RefuseServiceException("The volume id is not matched.");
		}
		return workerInfo.listBlockId();
	}

	@Override
	public List<Short> listFileByBid(short volumeId, int blockId)
			throws RefuseServiceException, TException {
		if (volumeId != workerInfo.getVolumeid()) {
			throw new RefuseServiceException("The volume id is not matched.");
		}
		if (!workerInfo.existBlock(blockId)) {
			throw new TException("The block is not found.");
		}
		return workerInfo.listFileId(blockId);
	}

	@Override
	public ClientFileInfo getClientFileInfo(long id)
			throws RefuseServiceException, TException {
		short fid = (short)(id & 0xFFFF);
		int bid = (int)(id >>> 16 & 0xFFFFFFFF);
		short vid = (short)(id >>> 48);
		
		if (vid != workerInfo.getVolumeid()) {
			throw new RefuseServiceException("The volume id is not matched.");
		}
		if (!workerInfo.existFile(bid, fid)) {
			throw new TException("The file is not found.");
		}
		
		ClientFileInfo info = new ClientFileInfo(id, 
				workerInfo.getFileLength(bid, fid), workerInfo.isInCache(bid, fid));
		return info;
	}

}
