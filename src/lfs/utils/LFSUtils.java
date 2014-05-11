package lfs.utils;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import lfs.common.Conf;
import lfs.common.UnderFileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class LFSUtils {
	private static final Logger LOG = Logger.getLogger(LFSUtils.class);
	public static String cacheFilePathFormat(short vid, int bid, short fid) {
		return Conf.getConfig().workerCacheDir + vid + "/" + bid + "/" + fid;
	}

	/**
	 * 
	 * @param vid
	 * @param bid
	 * @param infileid
	 * @param length
	 * @return
	 * @throws IOException
	 */
	public static ByteBuffer readMemCache(String path, int offset, int length) throws IOException {
		ByteBuffer bb = null;
		RandomAccessFile raf = new RandomAccessFile(path, "r");
		FileChannel channel = raf.getChannel();
		bb = channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
		channel.close();
		raf.close();
		return bb;
	}

	public static void writeMemCache(ByteBuffer bb, String path) throws IOException {
		String dirpath = path.substring(0, path.lastIndexOf('/'));
//		System.out.println(dirpath);
		File dir = new File(dirpath);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		File file = new File(path);
		if (file.exists()) {
			file.delete();
		}
		file.createNewFile();
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		byte[] btmp = new byte[8192];
//		int bOff = 0;
		int len = bb.remaining();
		int nRead = 0;
		while (nRead < len) {
			int min = Math.min(btmp.length, len - nRead);
			bb.get(btmp, 0, min);
			nRead += min;
			raf.write(btmp, 0, min);
		}
//		raf.write(bb.array());
		raf.close();
		bb.flip();
	}
	
	/**
	 * 
	 * @param path
	 * @param offInBlk
	 * @param offset
	 * @param length
	 * @return 返回可直接使用的bytebuffer
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static ByteBuffer readFromUnderFS(String path, int offInBlk, int offset, int length)
			throws IOException, URISyntaxException {
		LOG.debug("read UFS. path = " + path + ", offset = " + offset + ", length = " + length);
		UnderFileSystem ufs = UnderFileSystem.get(path);
		DataInputStream reader = new DataInputStream(ufs.open(path));
		byte[] buff = new byte[length];
		int off = offInBlk + offset;
		int skipNum = 0;
		while (skipNum < off) {
			skipNum += reader.skipBytes(off - skipNum);
		}
		int readNum = reader.read(buff);
		reader.close();
		if (readNum < 0)
			readNum = 0;
		ByteBuffer retBuffer = ByteBuffer.allocate(readNum);
		retBuffer.put(buff, 0, readNum).flip();
		return retBuffer;
	}

	public static void writeToUnderFS(List<String> srcPath, String dstPath)
			throws IOException, URISyntaxException {
		LOG.debug("write UFS. path = " + dstPath);
		UnderFileSystem ufs = UnderFileSystem.get(dstPath);
		DataOutputStream writer = new DataOutputStream(ufs.create(dstPath));
		for (String path : srcPath) {
			InputStream in = new BufferedInputStream(new FileInputStream(path));
			IOUtils.copyBytes(in, writer, 4096, false);
			in.close();
		}
		writer.close();
	}

	/**
	 * Change the local file to full permission.
	 * 
	 * @param file
	 *            that will be changed to full permission
	 * @throws IOException
	 */
	public static void changeLocalFileToFullPermission(String filePath)
			throws IOException {
		// set the full permission to everyone.
		List<String> commands = new ArrayList<String>();
		commands.add("/bin/chmod");
		commands.add("777");
		File file = new File(filePath);
		commands.add(file.getAbsolutePath());

		try {
			ProcessBuilder builder = new ProcessBuilder(commands);
			Process process = builder.start();

			redirectStreamAsync(process.getInputStream(), System.out);
			redirectStreamAsync(process.getErrorStream(), System.err);

			process.waitFor();

			if (process.exitValue() != 0) {
				throw new IOException(
						"Can not change the permission of the following file to '777':"
								+ file.getAbsolutePath());
			}
		} catch (InterruptedException e) {
			LOG.error(e.getMessage());
			throw new IOException(e);
		}
	}

	static void redirectStreamAsync(final InputStream input,
			final PrintStream output) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				Scanner scanner = new Scanner(input);
				while (scanner.hasNextLine()) {
					output.println(scanner.nextLine());
				}
				scanner.close();
			}
		}).start();
	}

	/**
	 * If the sticky bit of the 'file' is set, the 'file' is only writable to
	 * its owner and the owner of the folder containing the 'file'.
	 * 
	 * @param file
	 *            absolute file path
	 */
	public static void setLocalFileStickyBit(String file) {
		try {
			// sticky bit is not implemented in PosixFilePermission
			if (file.startsWith("/")) {
				Runtime.getRuntime().exec("chmod o+t " + file);
			}
		} catch (IOException e) {
			LOG.info("Can not set the sticky bit of the file : " + file);
		}
	}
	
	public static long calculateTotalCacheSpace() {
		File file = new File(Conf.getConfig().workerCacheDir);
		return file.getTotalSpace();
	}
	public static long calculateFreeCacheSpace() {
		File file = new File(Conf.getConfig().workerCacheDir);
		return file.getFreeSpace();
	}
	
	public static void cleanCacheByBlockId(short volumeId, int blkId) {
		File blockDir = new File(Conf.getConfig().workerCacheDir + volumeId + "/" + blkId + "/");
		File[] fileList = blockDir.listFiles();
		for (File file : fileList) {
			file.delete();
		}
		blockDir.delete();
	}
	
	public static void initLog4j() {
//		Properties prop = new Properties();
//
//		prop.setProperty("log4j.rootLogger", "INFO, CONSOLE");
//		prop.setProperty("log4j.appender.CONSOLE", "org.apache.log4j.ConsoleAppender");
//		prop.setProperty("log4j.appender.CONSOLE.layout", "org.apache.log4j.PatternLayout");
//		prop.setProperty("log4j.appender.CONSOLE.layout.ConversionPattern", "%d{HH:mm:ss,SSS} [%t] %-5p %C{1} : %m%n");
//
//		PropertyConfigurator.configure(prop);
		PropertyConfigurator.configure("conf/log4j.properties");
	}
}
