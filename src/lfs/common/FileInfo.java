package lfs.common;

public class FileInfo {
	public short	fileid;		//	file id in block file
	public int		offset;	//	offset in block file
	public int		size;	//	file size
	public int 	flag;	//	delete flag
	
	public FileInfo(short id) {
		this(id, 0, 0, 0);
	}

	public FileInfo(short fileid, int offset, int size, int flag) {
		super();
		this.fileid = fileid;
		this.offset = offset;
		this.size = size;
		this.flag = flag;
	}
	
}
