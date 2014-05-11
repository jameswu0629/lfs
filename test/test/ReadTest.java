package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;

import lfs.client.LFSClient;

public class ReadTest {

	public static void main(String[] args) throws IOException, TException {
		BufferedReader in = new BufferedReader(new FileReader("/home/paomian/lfs_hdfs.log"));
		List<Long> idList = new ArrayList<Long>();
		String line = in.readLine();
		while ((line = in.readLine()) != null) {
			if (!line.startsWith("[W]")) {
				continue;
			}
			String[] subArr = line.substring(3).split(";");
			idList.add(Long.parseLong(subArr[0].split("=")[1]));
		}
		in.close();
		PrintWriter out = new PrintWriter(new FileWriter("/home/paomian/lfs_hdfs.log", true));
		LFSClient client = LFSClient.get();
		out.println("## read");
		for (long id : idList) {
			long time = System.nanoTime();
			ByteBuffer buffer2 = client.read(id);
			time = System.nanoTime() - time;
			out.println("[R]" + "fid=" + id + ";\ttime=" + time + ";\tsize=" + buffer2.capacity());
			out.flush();
		}
		out.println("//~");
		out.close();
	}

}
