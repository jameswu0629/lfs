package lfs.common;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;


public class Conf {
//	public static final int BLOCK_SIZE = 64 * 1024 * 1024;
//	public static final String LOCAL_CACHE_DIR = "/mnt/tmpfs/";
//	public static final String PathSeparator = "/";
//	public static final String MASTER_HOST = "202.118.11.25";
//	public static final int MASTER_PORT = 9999;
//	public static final String UFS_ROOT_FOLDER = "hdfs://202.118.11.25/lfs/";
//	public static final long WORKER_CACHE_CAPACITY = 128 * 1024 * 1024;
//	public static final String JOURNAL_FOLDER = "hdfs://202.118.11.25/lfs/";
	
	public final String lfsDataDir;
	public final int blockSize;
	public final String workerCacheDir;
	public final int workerCacheSize;
	public final int workerPort;
	public final String masterHost;
	public final int masterPort;
	
	
	private Properties prop = new Properties();

	private static Conf conf = new Conf();
	
	private Conf() {
		BufferedInputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream("conf/config.properties"));
		} catch (FileNotFoundException e) {
		}
		try {
			if (in != null) {
				prop.load(in);
			}
		} catch (IOException e) {
		}
		
		lfsDataDir = dirPathFormat(prop.getProperty("lfs.data.dir", "hdfs://localhost/lfs/"));
		blockSize = parseKMGString(prop.getProperty("lfs.block.size", "64m"));
		workerCacheDir = dirPathFormat(prop.getProperty("lfs.worker.cache.dir", "/mnt/tmpfs/lfs/"));
		workerCacheSize = parseKMGString(prop.getProperty("lfs.worker.cache.size", "1g"));
		workerPort = parseKMGString(prop.getProperty("lfs.worker.port", "9998"));
		masterPort = parseKMGString(prop.getProperty("lfs.master.port", "9999"));
		masterHost = prop.getProperty("lfs.master.host", "localhost");
	}
	
	public static Conf getConfig() {
		return conf;
	}
	
	private static int parseKMGString(String str) {
		if (!str.matches("\\d+[kKmMgG]?")) {
			return 0;
		}
		if (str.matches("\\d+")) {
			return Integer.parseInt(str);
		}
		String subStr = str.substring(0, str.length() -1);
		int ret = Integer.parseInt(subStr);
		if (str.endsWith("k") || str.endsWith("K")) {
			ret *= 1024;
		} else if (str.endsWith("m") || str.endsWith("M")) {
			ret *= 1024 * 1024;
		} else {
			ret *= 1024 * 1024 * 1024;
		}
		return ret;
	}
	private String dirPathFormat(String path) {
		if (!path.endsWith("/")) {
			path += "/";
		}
		return path;
	}
}
