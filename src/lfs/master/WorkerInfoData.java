package lfs.master;

import lfs.thrift.NetAddress;
import lfs.thrift.WorkerData;

public class WorkerInfoData {
	public NetAddress addr;
	public short volumeId;
	public WorkerData data;
	public long lastUpdateTime;
//	public boolean isAlloced;
}
