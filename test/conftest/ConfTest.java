package conftest;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfTest {

	public static void main(String[] args) {
		BufferedInputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream("conf/config.properties"));
		} catch (FileNotFoundException e1) {
//			e1.printStackTrace();
		}
		Properties conf = new Properties();
		try {
			if (in != null) {
				conf.load(in);
			}
		} catch (IOException e) {
//			e.printStackTrace();
		}
		System.out.println(conf.getProperty("lfs.data.dir", "hdfs://localhost/lfs1"));
		System.out.println(conf.getProperty("lfs.meta.dir", ""));
		System.out.println(parseKMGString(conf.getProperty("lfs.block.size", "624m")));
		System.out.println(conf.getProperty("lfs.worker.cache.dir", "/mnt/tmpfs/lfs/1"));
		System.out.println(parseKMGString(conf.getProperty("lfs.worker.cache.size", "1g")));
		System.out.println(parseKMGString(conf.getProperty("lfs.worker.port", "99982")));
		System.out.println(conf.getProperty("lfs.master.port", "99992"));
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
}
