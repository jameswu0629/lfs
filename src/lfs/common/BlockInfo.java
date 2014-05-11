package lfs.common;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import lfs.utils.IDFactory;

public class BlockInfo {
	public int blockid;
	public int		size;
	public Map<Short, FileInfo> fileinfoMap = new TreeMap<Short, FileInfo>();
	private IDFactory idfactory = new IDFactory();
	public BlockInfo(int retid) {
		this.blockid = retid;
		this.size = 0;
	}
	
	public BlockInfo(int blockid, int size) {
		this.blockid = blockid;
		this.size = size;
	}

	public long genInterId() {
		return this.idfactory.generation();
	}
	
	public boolean exist(short id) {
		if (fileinfoMap.containsKey(id)) {
			return true;
		}
		return false;
	}
	
	public List<Short> listFileId() {
		List<Short> list = new LinkedList<Short>();
		for (Short s : fileinfoMap.keySet()) {
			list.add(s);
		}
		return list;
	}
}
