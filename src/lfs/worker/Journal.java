package lfs.worker;

import java.io.IOException;

import lfs.common.UnderFileSystem;

public class Journal {
	private EditLog mEditLog = null;

	private int mCurrentLogFileNum = 0;
	private String mImagePath;
	private String mStandbyImagePath = "";
	private String mEditLogPath;
	private UnderFileSystem mUFS;
	
	public Journal(String folder, String imageFileName, String editLogFileName) {
		if (!folder.endsWith("/")) {
			folder += "/";
		}
		mImagePath = folder + imageFileName;
		mEditLogPath = folder + editLogFileName;
	}
	public Journal(UnderFileSystem ufs, String folder, String imageFileName, String editLogFileName) {
		this(folder, imageFileName, editLogFileName);
		mUFS= ufs;
	}
	public long getImageModTime() throws IOException {
//		UnderFileSystem ufs = UnderFileSystem.get(mImagePath);
		UnderFileSystem ufs = mUFS;
		if (!ufs.exists(mImagePath)) {
			return -1;
		}
		return ufs.getModificationTimeMs(mImagePath);
	}

	public void loadImage(WorkerInfo info) throws IOException {
		Image.load(mUFS, info, mImagePath);
	}

	/**
	 * Load edit log.
	 * 
	 * @param info
	 *            The Master Info.
	 * @return The last transaction id.
	 * @throws IOException
	 */
	public long loadEditLog(WorkerInfo info) throws IOException {
		return EditLog.load(mUFS, info, mEditLogPath, mCurrentLogFileNum);
	}

	// public void loadSingleLogFile(WorkerInfo info, String path) throws
	// IOException {
	// EditLog.loadSingleLog(info, path);
	// mCurrentLogFileNum ++;
	// }

	public void createImage(WorkerInfo info) throws IOException {
		if (mStandbyImagePath == "") {
			Image.create(mUFS, info, mImagePath);
			EditLog.markUpToDate(mUFS, mEditLogPath);
		} else {
			Image.rename(mUFS, mStandbyImagePath, mImagePath);
		}
	}

	public void createImage(WorkerInfo info, String imagePath)
			throws IOException {
		Image.create(mUFS, info, imagePath);
		mStandbyImagePath = imagePath;
	}

	public void createEditLog(long transactionId) throws IOException {
		mEditLog = new EditLog(mUFS, mEditLogPath, transactionId);
	}

	public EditLog getEditLog() {
		return mEditLog;
	}

	/* Close down the edit log */
	public void close() {
		if (mEditLog != null) {
			mEditLog.close();
		}
	}

	/**
	 * Changes the max edit log size for testing purposes
	 * 
	 * @param size
	 */
	public void setMaxLogSize(int size) {
		mEditLog.setMaxLogSize(size);
	}
}
