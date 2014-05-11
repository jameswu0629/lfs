package lfs.client;

import lfs.thrift.ClientFileInfo;

public class LFile {
	private boolean isExist;
	private ClientFileInfo info;
	
	public LFile(boolean exist, ClientFileInfo info) {
		this.isExist = exist;
		this.info = info;
	}
	
	public boolean exist() {
		return isExist;
	}
	
	public int size() {
		return info.length;
	}
	
	public boolean isCached() {
		return info.isCached;
	}
}
