package lfs.utils;

import java.util.HashMap;

public class IDFactory {
	private long maxID = 0;
	public synchronized long generation() {
		long retid = ++ maxID;
		return retid;
	}
	
	public static void main(String[] args) {
		HashMap<Long, Integer> counter= new HashMap<Long, Integer>();
		for (int i = 0; i < 1024 * 64; i++) {
			long time = System.nanoTime();
//			time = time & 0xffff;
			if (counter.containsKey(time)) {
				int num = counter.get(time);
				counter.put(time, ++num);
				System.out.println(time + ";" + num);
			} else {
				counter.put(time, 1);
			}
		}
	}
}
