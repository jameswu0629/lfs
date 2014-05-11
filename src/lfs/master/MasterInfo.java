package lfs.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import lfs.common.Conf;
import lfs.common.Constants;
import lfs.common.UnderFileSystem;
import lfs.thrift.NetAddress;
import lfs.thrift.WorkerData;


public class MasterInfo {
	private static final Logger LOG = Logger.getLogger(MasterInfo.class);
//	private List<WorkerData> workerDataList = new ArrayList<WorkerData>();
	// volumeId 是否被分配 0 未使用 1使用
//	private HashMap<Short, Byte> volumeidMap = new HashMap<Short, Byte>();
	private Map<NetAddress, WorkerInfoData> workerMap = new TreeMap<NetAddress, WorkerInfoData>();
	private Map<Short, NetAddress> volumeIdMap = new TreeMap<Short, NetAddress>();
//	private int workerdataIndex = 0;	//	用于循环分配worker节点
//	private short maxVolumeId = 0;	// 用于递增分配volume id,
	private int reqCounter = 0;
	

	public MasterInfo() throws IOException {
		String rootFolder = Conf.getConfig().lfsDataDir;
		UnderFileSystem ufs = UnderFileSystem.get(rootFolder);
		if (!ufs.exists(rootFolder)) {
			ufs.mkdirs(rootFolder, true);
		}
		String[] list = ufs.list(rootFolder);
		String info = "";
		for (int i = 0; i < list.length; i ++) {
			short volumeId = Short.parseShort(list[i]);
			volumeIdMap.put(volumeId, Constants.EMPTY_ADDR);	//	表示未被分配
			info += list[i] + ", ";
		}		
		if (list == null || list.length == 0) {
			info = "null";
		} 
		LOG.info("扫描volume完成. volumes: " + info);
	}
	
	public synchronized NetAddress getVolume2WorkerAddress(short vid) {
		LOG.info("请求卷绑定的worker节点. 卷编号=" + vid + ".");
		return volumeIdMap.get(vid);
	}
	
	public synchronized NetAddress getWriteableWorkerAddress() {
		return getWorkerByIndex(reqCounter);
	}
	public synchronized NetAddress getWriteableWorkerAddressByHash(String fname) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * 按客户端地址hash值返回worker地址
	 * @param caddr
	 * @return
	 */
	public synchronized NetAddress getWriteableWorkerAddressByCAddr(NetAddress caddr) {
		int hashCode = caddr.hashCode();
		return getWorkerByIndex(hashCode);
	}
	
	private synchronized NetAddress getWorkerByIndex(int index) {
		LOG.info("请求可写worker节点. 总请求次数=" + reqCounter + ".");
		Object[] workers = workerMap.values().toArray(); 
		if (workers == null || workers.length == 0) {
			return null;
		}
		reqCounter ++;
		if (reqCounter < 0) {
			reqCounter = 0;
		}
		return ((WorkerInfoData)workers[index % workers.length]).addr;
	}
	
	public synchronized int getWorkerNumber() {
		return workerMap.size();
	}

	public synchronized WorkerData getWorkerData(short volumeId) {
		NetAddress addr = volumeIdMap.get(volumeId);
		if (addr.equals(Constants.EMPTY_ADDR)) {
			return null;
		}
		WorkerInfoData data = workerMap.get(addr);
		if (data == null) {
			return null;
		}
		return data.data;
	}

	public synchronized List<Short> getAllocedVolumeIdList() {
		List<Short> list = new ArrayList<Short>();
		Set<Short> keySet = volumeIdMap.keySet();
		for (Short s : keySet) {
			if (!volumeIdMap.get(s).equals(Constants.EMPTY_ADDR)) {
				list.add(s);
			}
		}
		return list;
	}

	public synchronized long registerWorker(NetAddress addr) {
		LOG.info("注册worker节点. " + addr + ".");
		if (workerMap.containsKey(addr)) {
			return -1;
		}
		WorkerInfoData data = new WorkerInfoData();
		data.addr = addr;
		data.volumeId = allocVolumeId(addr);
		data.lastUpdateTime = System.currentTimeMillis();
		data.data = null;
		workerMap.put(addr, data);
		
		return data.volumeId;
	}

	public synchronized byte updateWorkerInfo(NetAddress addr, WorkerData data) {
		if (!workerMap.containsKey(addr)) {
			return -1;
		}
		WorkerInfoData infoData = workerMap.get(addr);
		infoData.data = data;
		infoData.lastUpdateTime = System.currentTimeMillis();
		return 0;
	}


	private synchronized short allocVolumeId(NetAddress addr) {
		Set<Short> keys = volumeIdMap.keySet();
		short max = 0;
		for (Short s : keys) {
			if ( volumeIdMap.get(s).equals(addr)) {
				return s;
			}
			if (max < s) {
				max = s;
			}
		}
		for (Short s : keys) {
			if (volumeIdMap.get(s).equals(Constants.EMPTY_ADDR)) {
				volumeIdMap.put(s, addr);
				return s;
			}
		}
		max ++;
		volumeIdMap.put(max, addr);
		return max;
	}
	
	
	public void stop() {
	}
}
