package test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class StaticTest {
	public static void main(String[] args) throws IOException {
//		String path = "/tmp/test/file1";
//		String dirpath = path.substring(0, path.lastIndexOf('/'));
//		System.out.println(dirpath);
//		File dir = new File(dirpath);
//		if (!dir.exists()) {
//			dir.mkdirs();
//		}
//		File file = new File(path);
//		if (file.exists()) {
//			file.delete();
//		}
//		file.createNewFile();
//		RandomAccessFile raf = new RandomAccessFile(file, "rw");
//		raf.write(1);
//		raf.close();
		String[] list = new String[2];
		list[0] = "23";
		list[1] = "2345";
		System.out.println(list);
	}

}
