package test;

import java.net.InetSocketAddress;

public class TestUtils {
	public static void main(String[] args) {
		InetSocketAddress socket = new InetSocketAddress(9999);
		System.out.println(socket.getHostName());
	}
}
