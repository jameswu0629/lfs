package test;

import java.io.IOException;

import lfs.common.UnderFileSystem;

public class HdfsTest {

	public static void main(String[] args) throws IOException {
		UnderFileSystem  ufs = UnderFileSystem.get("hdfs://202.118.11.25/lfs");
//		String[] fileList = ufs.list("/lfs/1");
//		ufs.create("hdfs://202.118.11.25/test1.tmp");
//		ufs.create("/test2.tmp");
		ufs.delete("test2.tmp", true);
//		for (int i = 0; i < fileList.length; i++) {
//			System.out.println(fileList[i]);
//		}
	}

}
