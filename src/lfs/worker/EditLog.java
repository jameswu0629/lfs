package lfs.worker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;

import lfs.common.Constants;
import lfs.common.UnderFileSystem;

public class EditLog {
	private final static Logger LOG = Logger.getLogger(EditLog.class);

	private static final byte OP_ADD_BLOCK = 1;
	private static final byte OP_CREATE_FILE = 2;
	private static final byte OP_DELETE = 3;

	private static int mBackUpLogStartNum = -1;
	private static long mCurrentTId = 0;

	private final String PATH;

	private long mFlushedTransactionId = 0;
	private long mTransactionId = 0;
	private int mCurrentLogFileNum = 0;
	private int mMaxLogSize = 60 * Constants.MB;

	private UnderFileSystem UFS;
	private DataOutputStream DOS;
	private OutputStream OS;

	/**
	 * 载入completed/中log文件，返回最大记录编号
	 * @param info
	 * @param path
	 *            log_root_path + "log.data"
	 * @param currentLogFileNum
	 * @return
	 * @throws IOException
	 */
//	public static long load(WorkerInfo info, String path, int currentLogFileNum)
//			throws IOException {
//		UnderFileSystem ufs = UnderFileSystem.get(path);
//		if (!ufs.exists(path)) {
//			LOG.info("Edit Log " + path + " does not exist.");
//			return 0;
//		}
//		LOG.info("currentLogNum passed in was " + currentLogFileNum);
//		int completedLogs = currentLogFileNum;
//		mBackUpLogStartNum = currentLogFileNum;
//		int numFiles = 1;
//		String completedPath = path.substring(0, path.lastIndexOf("/"))
//				+ "/completed";
//		if (!ufs.exists(completedPath)) {
//			LOG.info("No completed edit logs to be parsed");
//		} else {
//			while (ufs.exists(completedPath + "/" + (completedLogs++)
//					+ ".editLog")) {
//				numFiles++;
//			}
//		}
//		String editLogs[] = new String[numFiles];
//		for (int i = 0; i < numFiles; i++) {
//			if (i != numFiles - 1) {
//				editLogs[i] = completedPath + "/" + (i + currentLogFileNum)
//						+ ".editLog";
//			} else {
//				editLogs[i] = path;
//			}
//		}
//
//		for (String currentPath : editLogs) {
//			LOG.info("Loading Edit Log " + currentPath);
//			loadSingleLog(info, currentPath);
//		}
//		ufs.close();
//		return mCurrentTId;
//	}

	public static long load(UnderFileSystem ufs, WorkerInfo info, String path, int currentLogFileNum) throws IOException {
		if (!ufs.exists(path)) {
			LOG.info("Edit Log " + path + " does not exist.");
			return 0;
		}
		LOG.info("currentLogNum passed in was " + currentLogFileNum);
		int completedLogs = currentLogFileNum;
		mBackUpLogStartNum = currentLogFileNum;
		int numFiles = 1;
		String completedPath = path.substring(0, path.lastIndexOf("/"))
				+ "/completed";
		if (!ufs.exists(completedPath)) {
			LOG.info("No completed edit logs to be parsed");
		} else {
			while (ufs.exists(completedPath + "/" + (completedLogs++)
					+ ".editLog")) {
				numFiles++;
			}
		}
		String editLogs[] = new String[numFiles];
		for (int i = 0; i < numFiles; i++) {
			if (i != numFiles - 1) {
				editLogs[i] = completedPath + "/" + (i + currentLogFileNum)
						+ ".editLog";
			} else {
				editLogs[i] = path;
			}
		}

		for (String currentPath : editLogs) {
			LOG.info("Loading Edit Log " + currentPath);
			loadSingleLog(ufs, info, currentPath);
		}
		return mCurrentTId;
	}
	
	/**
	 * 
	 * @param info
	 * @param path
	 *            log_root_path + "completed/" + "num.editLog"
	 * @throws IOException
	 */
//	public static void loadSingleLog(WorkerInfo info, String path)
//			throws IOException {
//		UnderFileSystem ufs = UnderFileSystem.get(path);
//
//		DataInputStream is = new DataInputStream(ufs.open(path));
//		while (true) {
//			long tid;
//			try {
//				tid = is.readLong();
//			} catch (EOFException e) {
//				break;
//			}
//
//			mCurrentTId = tid;
//			byte op = is.readByte();
//			if (op == EditLog.OP_CREATE_FILE) {
//				info.addFile(is.readInt(), is.readShort(), is.readInt(),
//						is.readInt(), is.readInt());
//			} else {
//				throw new IOException("Invalid op type " + op);
//			}
//		}
//		is.close();
//		ufs.close();
//	}

	public static void loadSingleLog(UnderFileSystem ufs, WorkerInfo info, String path) throws IOException {
		DataInputStream is = new DataInputStream(ufs.open(path));
		while (true) {
			long tid;
			try {
				tid = is.readLong();
			} catch (EOFException e) {
				break;
			}

			mCurrentTId = tid;
			byte op = is.readByte();
			if (op == EditLog.OP_CREATE_FILE) {
				info.addFile(is.readInt(), is.readShort(), is.readInt(),
						is.readInt(), is.readInt());
			} else {
				throw new IOException("Invalid op type " + op);
			}
		}
		is.close();
	}
	/**
	 * 
	 * @param path
	 * @param transactionId
	 * @throws IOException
	 */
//	public EditLog(String path, long transactionId) throws IOException {
//		LOG.info("Creating edit log file " + path);
//		PATH = path;
//		UFS = UnderFileSystem.get(path);
//		if (mBackUpLogStartNum != -1) {
//			String folder = path.substring(0, path.lastIndexOf("/"))
//					+ "/completed";
//			LOG.info("Deleting completed editlogs that are part of the image.");
//			deleteCompletedLogs(path, mBackUpLogStartNum);
//			LOG.info("Backing up logs from " + mBackUpLogStartNum
//					+ " since image is not updated.");
//			UFS.mkdirs(folder, true);
//			String toRename = folder + "/" + mBackUpLogStartNum + ".editLog";
//			int mCurrentLogFileNum = 0;
//			while (UFS.exists(toRename)) {
//				LOG.info("Rename " + toRename + " to " + folder + "/"
//						+ mCurrentLogFileNum + ".editLog");
//				mCurrentLogFileNum++;
//				mBackUpLogStartNum++;
//				toRename = folder + "/" + mBackUpLogStartNum + ".editLog";
//			}
//			if (UFS.exists(path)) {
//				UFS.rename(path, folder + "/" + mCurrentLogFileNum + ".editLog");
//				LOG.info("Rename " + path + " to " + folder + "/"
//						+ mCurrentLogFileNum + ".editLog");
//				mCurrentLogFileNum++;
//			}
//			mBackUpLogStartNum = -1;
//		}
//		OS = UFS.create(path);
//		DOS = new DataOutputStream(OS);
//		LOG.info("Created file " + path);
//		mFlushedTransactionId = transactionId;
//		mTransactionId = transactionId;
//
//	}

	public EditLog(UnderFileSystem ufs, String path, long transactionId) throws IOException {
		LOG.info("Creating edit log file " + path);
		PATH = path;
		UFS = ufs;
		if (mBackUpLogStartNum != -1) {
			String folder = path.substring(0, path.lastIndexOf("/"))
					+ "/completed";
			LOG.info("Deleting completed editlogs that are part of the image.");
			deleteCompletedLogs(UFS, path, mBackUpLogStartNum);
			LOG.info("Backing up logs from " + mBackUpLogStartNum
					+ " since image is not updated.");
			UFS.mkdirs(folder, true);
			String toRename = folder + "/" + mBackUpLogStartNum + ".editLog";
			int mCurrentLogFileNum = 0;
			while (UFS.exists(toRename)) {
				LOG.info("Rename " + toRename + " to " + folder + "/"
						+ mCurrentLogFileNum + ".editLog");
				mCurrentLogFileNum++;
				mBackUpLogStartNum++;
				toRename = folder + "/" + mBackUpLogStartNum + ".editLog";
			}
			if (UFS.exists(path)) {
				UFS.rename(path, folder + "/" + mCurrentLogFileNum + ".editLog");
				LOG.info("Rename " + path + " to " + folder + "/"
						+ mCurrentLogFileNum + ".editLog");
				mCurrentLogFileNum++;
			}
			mBackUpLogStartNum = -1;
		}
		OS = UFS.create(path);
		DOS = new DataOutputStream(OS);
		LOG.info("Created file " + path);
		mFlushedTransactionId = transactionId;
		mTransactionId = transactionId;

	}
	
//	public static void deleteCompletedLogs(String path, int upTo) {
//		UnderFileSystem ufs = UnderFileSystem.get(path);
//		String folder = path.substring(0, path.lastIndexOf("/")) + "/completed";
//		try {
//			for (int i = 0; i < upTo; i++) {
//				String toDelete = folder + "/" + i + ".editLog";
//				LOG.info("Deleting editlog " + toDelete);
//				ufs.delete(toDelete, true);
//			}
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//	}

	public static void deleteCompletedLogs(UnderFileSystem ufs, String path, int upTo) {
//		UnderFileSystem ufs = UnderFileSystem.get(path);
		String folder = path.substring(0, path.lastIndexOf("/")) + "/completed";
		try {
			for (int i = 0; i < upTo; i++) {
				String toDelete = folder + "/" + i + ".editLog";
				LOG.info("Deleting editlog " + toDelete);
				ufs.delete(toDelete, true);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * delete editlog in completed/
	 * 
	 * @param path
	 */
//	public static void markUpToDate(String path) {
//		UnderFileSystem ufs = UnderFileSystem.get(path);
//		String folder = path.substring(0, path.lastIndexOf("/")) + "/completed";
//		try {
//			// delete all loaded editlogs since mBackupLogStartNum.
//			String toDelete = folder + "/" + mBackUpLogStartNum + ".editLog";
//			while (ufs.exists(toDelete)) {
//				LOG.info("Deleting editlog " + toDelete);
//				ufs.delete(toDelete, true);
//				mBackUpLogStartNum++;
//				toDelete = folder + "/" + mBackUpLogStartNum + ".editLog";
//			}
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//		mBackUpLogStartNum = -1;
//	}
	
	public static void markUpToDate(UnderFileSystem ufs, String path) {
//		UnderFileSystem ufs = UnderFileSystem.get(path);
		String folder = path.substring(0, path.lastIndexOf("/")) + "/completed";
		try {
			// delete all loaded editlogs since mBackupLogStartNum.
			String toDelete = folder + "/" + mBackUpLogStartNum + ".editLog";
			while (ufs.exists(toDelete)) {
				LOG.info("Deleting editlog " + toDelete);
				ufs.delete(toDelete, true);
				mBackUpLogStartNum++;
				toDelete = folder + "/" + mBackUpLogStartNum + ".editLog";
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		mBackUpLogStartNum = -1;
	}

	public void rotateEditLog(String path) {
		_close();
		LOG.info("Edit log max size reached, rotating edit log");
		String pathPrefix = path.substring(0, path.lastIndexOf("/"))
				+ "/completed";
		LOG.info("path: " + path + " prefix: " + pathPrefix);
		try {
			if (!UFS.exists(pathPrefix)) {
				UFS.mkdirs(pathPrefix, true);
			}
			String newPath = pathPrefix + "/" + (mCurrentLogFileNum++)
					+ ".editLog";
			UFS.rename(path, newPath);
			LOG.info("Renamed " + path + " to " + newPath);
			OS = UFS.create(path);
			DOS = new DataOutputStream(OS);
			LOG.info("Created new log file " + path);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void createFile(int blockid, short fileid, int offset,
			int size, int flag) throws IOException {
		DOS.writeLong(++mTransactionId);
		DOS.writeByte(EditLog.OP_CREATE_FILE);
		DOS.writeInt(blockid);
		DOS.writeShort(fileid);
		DOS.writeInt(offset);
		DOS.writeInt(size);
		DOS.writeInt(flag);
	}

	/**
	 * Flush the log onto the storage.
	 */
	public synchronized void flush() {
		try {
			DOS.flush();
			if (OS instanceof FSDataOutputStream) {
				((FSDataOutputStream) OS).sync();
			}
			if (DOS.size() > mMaxLogSize) {
				rotateEditLog(PATH);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		mFlushedTransactionId = mTransactionId;
	}

	/**
	 * Only close those outputStreams.
	 */
	private synchronized void _close() {
		try {
			if (DOS != null) {
				DOS.close();
			}
			if (OS != null) {
				OS.close();
			}
		} catch (IOException e) {
			LOG.info(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	/**
	 * Close the log.
	 */
	public synchronized void close() {
//		try {
			_close();
//			UFS.close();
//		} catch (IOException e) {
//			LOG.info(e.getMessage());
//			throw new RuntimeException(e);
//		}
	}
	
	  /**
	   * Changes the max log size for testing purposes.
	   * @param size
	   */
	  public void setMaxLogSize(int size) {
	    mMaxLogSize = size;
	  }
}
