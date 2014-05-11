package lfs.worker;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import lfs.common.BlockInfo;
import lfs.common.Conf;
import lfs.common.FileInfo;
import lfs.utils.LFSUtils;

import org.apache.log4j.Logger;


public class BlockMerge implements Runnable{
	private static final Logger LOG = Logger.getLogger(BlockMerge.class);
	
	private WorkerInfo workerinfo;
	public BlockMerge(WorkerInfo workerinfo) {
		this.workerinfo = workerinfo;
	}
	@Override
	public void run() {
		while (true) {
			int blkid = 0;
			try {
				blkid = this.workerinfo.getMergeBlockQueue().take();
			} catch (InterruptedException e) {
				LOG.error(e);
				continue;
			}
			
			BlockInfo blockinfo = this.workerinfo.getBlockinfoMap().get(blkid);
			List<String> fileList = new ArrayList<String>();
			String srcPath = Conf.getConfig().workerCacheDir  
					+ this.workerinfo.getVolumeid() + "/"
					+ blkid + "/";
			for (FileInfo fileinfo : blockinfo.fileinfoMap.values()) {
				fileList.add(srcPath + fileinfo.fileid);
			}
			String dstPath = Conf.getConfig().lfsDataDir  
					+ this.workerinfo.getVolumeid() + "/"
					+ blkid;
			try {
				LFSUtils.writeToUnderFS(fileList, dstPath);
			} catch (IOException e) {
				LOG.error(e.getMessage());
			} catch (URISyntaxException e) {
				LOG.error(e.getMessage());
			}
			for (String s : fileList) {
				File file = new File(s);
				file.delete();
			}
//			this.workerinfo.addUsage(blockinfo.size);
		}
	}

}
