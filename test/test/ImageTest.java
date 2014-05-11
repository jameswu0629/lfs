package test;

import java.io.DataInputStream;
import java.io.IOException;

import lfs.common.Conf;
import lfs.common.UnderFileSystem;

public class ImageTest {

	public static void main(String[] args) throws IOException {
		String rootFolder = Conf.getConfig().lfsDataDir;
		UnderFileSystem ufs = UnderFileSystem.get(rootFolder);
		if (!ufs.exists(rootFolder)) {
			ufs.mkdirs(rootFolder, true);
		}
		String[] list = ufs.list(rootFolder);
		for (int i = 0; i < list.length; i ++) {
			System.out.println(list[i]);
		}
	}

}
