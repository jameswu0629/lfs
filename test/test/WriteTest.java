package test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import lfs.client.LFSClient;
import lfs.utils.LFSUtils;

public class WriteTest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws TException 
	 */
	public static void main(String[] args) throws IOException, TException {
		LFSUtils.initLog4j();
		
		File mp3Dir  = new File("/media/2A16A49216A46115/Downloads/music/mp3/");
		File[] mp3Files = mp3Dir.listFiles();
		
//		for (File f : mp3Files) {
//			System.out.println(f.getAbsolutePath());
//		}
		PrintWriter out = new PrintWriter(new FileWriter("/home/paomian/lfs_hdfs.log", true));
		LFSClient client = LFSClient.get();

		int count = mp3Files.length;


		
		out.println("##write hdfs");
		for (int i = 0; i< count; i ++) {
//			File file = new File("/home/paomian/下载/1.jpg");
			File file = mp3Files[i];
			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			byte[] buffer = new byte[(int) file.length()];
			in.read(buffer);
			
			long time = System.nanoTime();
			long id = client.write(buffer);
			time = System.nanoTime() - time;
			out.println("[W]fid=" + id + ";\ttime=" + time + ";\tsize=" + file.length());
			out.flush();
		}
		out.println("//~");
		out.close();
		
	}
//	281474976776193
}
