package test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;



public class RemoveList {
	public Object closeObject = new Object();
	public static void main(String[] args) throws IOException, InterruptedException {
		RemoveList rm = new RemoveList();
		rm.start();
		Thread.sleep(1000);
		new Thread(new ThreadA(rm.closeObject)).start();
	}
	public void start() {
		new Thread(new HandleClose(this, this.closeObject)).start();
	}
	public class HandleClose implements Runnable {
		private Object closeObj;
		private RemoveList rm;
		public HandleClose(RemoveList rm, Object obj) {
			closeObj = obj;
			this.rm = rm;
		}
		@Override
		public void run() {
			synchronized (closeObj) {
					closeObj.notify();;

			}
			rm.stop();
		}
		
	}
	public void stop() {
		System.out.println("stop");
	}
}

class ThreadA implements Runnable {
	public Object closeObj;
	public ThreadA(Object obj) {
		closeObj =obj;
	}
	private volatile boolean isStop = false;
	public void close() {
		isStop = true;
	}
	@Override
	public void run() {
		synchronized(closeObj) {
			closeObj.notify();
		}
	}
	
}
