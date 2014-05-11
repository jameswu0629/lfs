package lfs.common;

import lfs.thrift.NetAddress;

public class Constants {
	public static final int KB = 1024;
	public static final int MB = KB * 1024;
	public static final int GB = MB * 1024;
	public static final long TB = GB * 1024L;
	public static final long CMD_CLOSE = -1L;
	public static final NetAddress EMPTY_ADDR = new NetAddress("", 0);
}
