package lfs.worker;

import java.util.HashMap;

import lfs.common.BlockInfo;
import lfs.common.FileInfo;

public class BlockFactory {
	private int lastBlockid = 0;
	private HashMap<Integer, FileFactory> fileFactoryMap = new HashMap<Integer, FileFactory>();
	
	public BlockFactory(int blkId) {
		lastBlockid = blkId;
	}
	public BlockInfo createBlockInfo() {
		BlockInfo blockinfo = new BlockInfo(++lastBlockid); 
		return blockinfo;
	}
	public FileInfo createFileInfo(int blockid, int offset, int len) {
		FileInfo info = getFileFactory(blockid).createFileInfo();
		info.offset = offset;
		info.size = len;
		info.flag = 0;
		return info;
	}
	public FileFactory getFileFactory(int blockid) {
		if (!this.fileFactoryMap.containsKey(blockid)) {
			this.fileFactoryMap.put(blockid, new FileFactory());
		}
		return this.fileFactoryMap.get(blockid);
	}
	
	class FileFactory {
		private short lastFileid = 0 ;
		public FileInfo createFileInfo(){
			FileInfo fileinfo = new FileInfo(++lastFileid);
			return fileinfo;
		}
	}
}
