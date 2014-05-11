package lfs.worker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import lfs.common.BlockInfo;
import lfs.common.Conf;
import lfs.common.FileInfo;
import lfs.common.UnderFileSystem;
import lfs.utils.LFSUtils;

public class WorkerInfo {
	private static final Logger LOG = Logger.getLogger(WorkerInfo.class);
	
	private short volumeId;
//	private long capacity;
//	private long usage;
	
	private HashMap<Integer, BlockInfo> blockinfoMap = new HashMap<Integer, BlockInfo>();
//	private HashSet<Long> inMemFileSet = new HashSet<Long>();
	private HashMap<Integer, Set<Short>> inCacheFileIdMap = new HashMap<Integer, Set<Short>>();
	private List<Integer> writeableBlockList = new LinkedList<Integer>();
	private LinkedBlockingQueue<Integer> mergeBlockQueue = new LinkedBlockingQueue<Integer>();
	
	private BlockFactory blockfactory;
	private int writeableBlockId;
	private Journal journal;
	
	private UnderFileSystem ufs;
	private DataOutputStream dOut; 
	
	public WorkerInfo(short volumeid, Journal journal, UnderFileSystem ufs) throws IOException {
		this.volumeId = volumeid;
		this.journal = journal;
		this.ufs = ufs;
		// 文件元数据恢复
		initMeta();
		// 块工厂恢复
		int lastBlockid = 0;
		Set<Integer> blkKey = blockinfoMap.keySet();
		for (int blkId : blkKey) {
			if (lastBlockid < blkId) {
				lastBlockid = blkId;
			}
		}
		blockfactory = new BlockFactory(lastBlockid);
	}
	
	public HashMap<Integer, BlockInfo> getBlockinfoMap() {
		return blockinfoMap;
	}

	public void setBlockinfoMap(HashMap<Integer, BlockInfo> blockinfoMap) {
		this.blockinfoMap = blockinfoMap;
	}

	public short getVolumeid() {
		return volumeId;
	}

	public LinkedBlockingQueue<Integer> getMergeBlockQueue() {
		return mergeBlockQueue;
	}
	
	public void putIdCache(int blkId, short fileId) {
		if (!inCacheFileIdMap.containsKey(blkId)) {
			Set<Short> set = new HashSet<Short>();
			set.add(fileId);
			inCacheFileIdMap.put(blkId, set);
		} else {
			inCacheFileIdMap.get(blkId).add(fileId);
		}
		
	}
	
	/**
	 *  初始化元数据
	 * @throws IOException
	 */
	public void initMeta() throws IOException {
		journal.loadImage(this);
		long tid = journal.loadEditLog(this);
		
		journal.createImage(this);
		journal.createEditLog(tid);
	}
	
	/**
	 * 检查文件是否在内存中
	 * @param blockid
	 * @param infileid
	 * @return
	 */
	public boolean isInCache(int blockid, short infileid) {
		if (!inCacheFileIdMap.containsKey(blockid)) {
			return false;
		}
		if (!inCacheFileIdMap.get(blockid).contains(infileid)) {
			return false;
		}
		return true;
	}
	public boolean existBlock(int blockId) {
		if (blockinfoMap.containsKey(blockId)) {
			return true;
		}
		return false;
	}
	public boolean existFile(int blockId, short fileId) {
		if (existBlock(blockId)) {
			BlockInfo blockInfo = blockinfoMap.get(blockId);
			if (blockInfo.exist(fileId)) {
				return true;
			}
		}
		return false;
	}
	
	public int getFileLength(int blockId, short fileId) {
		BlockInfo blockInfo = blockinfoMap.get(blockId);
		FileInfo fileInfo = blockInfo.fileinfoMap.get(fileId);
		return fileInfo.size;
	}
	
	public List<Integer> listBlockId() {
		List<Integer> list = new LinkedList<Integer>();
		for (int id : blockinfoMap.keySet()) {
			list.add(id);
		}
		return list;
	}
	
	public List<Short> listFileId(int blockId) {
		BlockInfo block = blockinfoMap.get(blockId);
		return block.listFileId();
	}
	
	public long store2(ByteBuffer buffer) throws IOException {
		int len = buffer.remaining();
	
		//找可写len文件的块
		int blkId = 0;
		if (writeableBlockId > 0) {
			BlockInfo block = blockinfoMap.get(writeableBlockId);
			if (block.size + len < Conf.getConfig().blockSize) {
				blkId = block.blockid;
			}
		}
		if (blkId == 0) {
			//没有找到可写len文件的块，但是存在其他可写块，则需要关闭可写块到底层文件系统的输出流
			if (writeableBlockId > 0) {
				dOut.close();
//				ufs.close();
			}
			//创建可写块
			BlockInfo block = blockfactory.createBlockInfo();
			blockinfoMap.put(block.blockid, block);
			writeableBlockId = block.blockid;
			blkId = block.blockid;
			//创建可写块到底层文件系统的输出流
			String blockPath = Conf.getConfig().lfsDataDir + volumeId + "/" + blkId;
//			ufs = UnderFileSystem.get(blockPath);
			dOut = new DataOutputStream(ufs.create(blockPath));
		}
		//创建文件元数据
		FileInfo fileInfo = blockfactory.createFileInfo(blkId, blockinfoMap.get(blkId).size, len);
		blockinfoMap.get(blkId).fileinfoMap.put(fileInfo.fileid, fileInfo);
		blockinfoMap.get(blkId).size += len;
		
		//找到或者新建了可写len文件的块, 写文件到底层文件系统
		byte[] btmp = new byte[8192];
//		int bOff = 0;
		int nRead = 0;
		while (nRead < len) {
			int min = Math.min(btmp.length, len - nRead);
			buffer.get(btmp, 0, min);
			nRead += min;
			dOut.write(btmp, 0, min);
		}
//		dOut.write(buffer.array());
		dOut.flush();
		//写Log
		journal.getEditLog().createFile(blkId, fileInfo.fileid, fileInfo.offset, fileInfo.size, fileInfo.flag);
		journal.getEditLog().flush();
		
		buffer.flip();
		putFileCache(buffer, volumeId, blkId, fileInfo.fileid);
		//全局编号
		long retId = (long)volumeId << 48 | (long)blkId << 16 | (long)fileInfo.fileid;
		
		return retId;
	}
	private void putFileCache(ByteBuffer buffer, short vid, int bid, short fid) throws IOException {
		//清理缓存
		if (LFSUtils.calculateFreeCacheSpace() < buffer.remaining()) {
			cleanCache();
		}
		//写文件到缓存
		LFSUtils.writeMemCache(buffer, LFSUtils.cacheFilePathFormat(vid, bid, fid));
		//加入缓存映射
		putIdCache(bid, fid);
		LOG.debug("add cache. vid = " + vid + "; bid = " + bid + "; fid = " + fid);
	}
	public void cleanCache() {
		Set<Integer> blkSet = inCacheFileIdMap.keySet();
		for (int blkId : blkSet) {
			if (blkId != writeableBlockId) {
				LFSUtils.cleanCacheByBlockId(volumeId, blkId);
				inCacheFileIdMap.remove(blkId);
				break;
			}
		}
	}
	/**
	 * 从内存或者底层文件系统取文件，并替换缓存。
	 * @param blockid
	 * @param infileid
	 * @param offset	希望读取的起始地址
	 * @param length	希望读取的文件内容长度
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public ByteBuffer fetch(int blockid, short infileid, int offset, int length) throws IOException, URISyntaxException {
		ByteBuffer retbuf = null;
		if (!blockinfoMap.containsKey(blockid)) {
			throw new IOException("块编号: " + blockid + ",丢失或者不存在。");
		}
		BlockInfo block = blockinfoMap.get(blockid);
		if (!block.fileinfoMap.containsKey(infileid)) {
			throw new IOException("内部文件编号: " + infileid + ",丢失或者不存在。");
		}
		if (length <= 0) {
			length = block.fileinfoMap.get(infileid).size;
		}
		if (isInCache(blockid, infileid)) {
			try {
				retbuf = LFSUtils.readMemCache(LFSUtils.cacheFilePathFormat(volumeId, blockid, infileid), offset, length);
			} catch (IOException e) {
				LOG.error(e.getMessage());
				throw e;
			}
		} else {
			//块内偏移量
			int offInBlk = block.fileinfoMap.get(infileid).offset;
			try {
				retbuf = LFSUtils.readFromUnderFS(Conf.getConfig().lfsDataDir + volumeId + "/" + blockid,
						offInBlk, offset, length);
			} catch (URISyntaxException e) {
				LOG.error(e.getMessage());
				throw e;
			}
		
			putFileCache(retbuf,volumeId, blockid, infileid);
		}
		return retbuf;
	}
	
	public void flush() {
		for (int bid : writeableBlockList) {
			mergeBlockQueue.add(bid);
		}
		while (!mergeBlockQueue.isEmpty() || !isDeletedCacheFiles()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		
	}
	
	private boolean isDeletedCacheFiles() {
		for (int bid : writeableBlockList) {
			BlockInfo block = getBlockinfoMap().get(bid);
			List<String> fileList = new ArrayList<String>();
			String path = Conf.getConfig().workerCacheDir  + volumeId + "/" + bid + "/";
			for (FileInfo fileinfo : block.fileinfoMap.values()) {
				fileList.add(path + fileinfo.fileid);
			}
			for (String s : fileList) {
				File file = new File(s);
				if (file.exists()) {
					return false;
				}
			}
		}
		return true;
	}
	//////////////////////////////
	//	 以下用于日志处理	//////////
	//////////////////////////////
	public void addBlock(int blockid, int size) {
		if (!blockinfoMap.containsKey(blockid)) {
			BlockInfo block = new BlockInfo(blockid, size);
			blockinfoMap.put(blockid, block);
		}
	}
	
	public void addFile(int blockid, short fid, int offset, int size, int flag) {
		BlockInfo block;
		if (!blockinfoMap.containsKey(blockid)) {
			block = new BlockInfo(blockid);
			blockinfoMap.put(blockid, block);
		} else {
			block = blockinfoMap.get(blockid);
		}
		block.fileinfoMap.put(fid, new FileInfo(fid, offset, size, flag));
		block.size += size;
	}

	public void loadImage(DataInputStream in) throws IOException {
		while (true) {
			int blockid;
			short fileid;
			int offset;
			int size;
			int flag;
			try {
				blockid = in.readInt();
				fileid = in.readShort();
				offset = in.readInt();
				size = in.readInt();
				flag = in.readInt();
			} catch (EOFException e) {
				break;
			}
			addFile(blockid, fileid, offset, size, flag);
		}
	}

	public void createImage(DataOutputStream out) throws IOException {
		Set<Integer> blockKeySet = blockinfoMap.keySet();
		for (int blockKey : blockKeySet) {
			BlockInfo block = blockinfoMap.get(blockKey);
			Set<Short> fileKeySet = block.fileinfoMap.keySet();
			for (short fileKey : fileKeySet) {
				FileInfo file = block.fileinfoMap.get(fileKey);
				out.writeInt(block.blockid);
				out.writeShort(file.fileid);
				out.writeInt(file.offset);
				out.writeInt(file.size);
				out.writeInt(file.flag);
			}
		}
	}

//	public void addUsage(int size) {
//		usage += size;
//	}
}
