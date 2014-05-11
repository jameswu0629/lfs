package lfs.worker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import lfs.common.UnderFileSystem;

import org.apache.log4j.Logger;

public class Image {
	private final static Logger LOG = Logger.getLogger(Image.class);

	/**
	 * 从path载入元数据
	 * @param info
	 * @param path
	 * @throws IOException
	 */
//	public static void load(WorkerInfo info, String path) throws IOException {
//		UnderFileSystem ufs = UnderFileSystem.get(path);
//		if (!ufs.exists(path)) {
//			LOG.info("Image " + path + " does not exist.");
//			return;
//		}
//		LOG.info("Loading image " + path);
//		DataInputStream imageIs = new DataInputStream(ufs.open(path));
//
//		// int tVersion = imageIs.readInt();
//		// if (tVersion != Constants.JOURNAL_VERSION) {
//		// throw new IOException("Image " + path + " has journal version " +
//		// tVersion + " ." +
//		// "The system has verion " + Constants.JOURNAL_VERSION);
//		// }
//
//		info.loadImage(imageIs);
//		imageIs.close();
//		ufs.close();
//	}
	public static void load(UnderFileSystem ufs, WorkerInfo info, String path) throws IOException {
		if (!ufs.exists(path)) {
			LOG.info("Image " + path + " does not exist.");
			return;
		}
		LOG.info("Loading image " + path);
		DataInputStream imageIs = new DataInputStream(ufs.open(path));
		info.loadImage(imageIs);
		imageIs.close();
	}
	/**
	 * 写元数据到path，非线程安全
	 * @param info
	 * @param path
	 * @throws IOException
	 */
//	public static void create(WorkerInfo info, String path) throws IOException {
//		String tPath = path + ".tmp";
//		String parentFolder = path.substring(0, path.lastIndexOf("/"));
//		LOG.info("Creating the image file: " + tPath);
//		UnderFileSystem ufs = UnderFileSystem.get(path);
//		if (!ufs.exists(parentFolder)) {
//			LOG.info("Creating parent folder " + parentFolder);
//			ufs.mkdirs(parentFolder, true);
//		}
//		OutputStream os = ufs.create(tPath);
//		DataOutputStream imageOs = new DataOutputStream(os);
//
//		// imageOs.writeInt(Constants.JOURNAL_VERSION);
//		info.createImage(imageOs);
//		imageOs.flush();
//		imageOs.close();
//
//		LOG.info("Succefully created the image file: " + tPath);
//		ufs.delete(path, false);
//		ufs.rename(tPath, path);
//		ufs.delete(tPath, false);
//		LOG.info("Renamed " + tPath + " to " + path);
//		// safe to close, nothing created here with scope outside function
//		ufs.close();
//	}
	
	public static void create(UnderFileSystem ufs, WorkerInfo info, String path) throws IOException {
		String tPath = path + ".tmp";
		String parentFolder = path.substring(0, path.lastIndexOf("/"));
		LOG.info("Creating the image file: " + tPath);
		if (!ufs.exists(parentFolder)) {
			LOG.info("Creating parent folder " + parentFolder);
			ufs.mkdirs(parentFolder, true);
		}
		OutputStream os = ufs.create(tPath);
		DataOutputStream imageOs = new DataOutputStream(os);

		// imageOs.writeInt(Constants.JOURNAL_VERSION);
		info.createImage(imageOs);
		imageOs.flush();
		imageOs.close();

		LOG.info("Succefully created the image file: " + tPath);
		ufs.delete(path, false);
		ufs.rename(tPath, path);
		ufs.delete(tPath, false);
		LOG.info("Renamed " + tPath + " to " + path);
	}
	
//	public static void rename(String src, String dst) throws IOException {
//		UnderFileSystem ufs = UnderFileSystem.get(src);
//		ufs.rename(src, dst);
//		LOG.info("Renamed " + src + " to " + dst);
//	}
	
	public static void rename(UnderFileSystem ufs, String src, String dst) throws IOException {
		ufs.rename(src, dst);
		LOG.info("Renamed " + src + " to " + dst);
	}
}
