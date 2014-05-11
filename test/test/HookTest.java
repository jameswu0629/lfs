package test;

public class HookTest {
	public static boolean isStop =false;
	public static void main(String[] args) {
		new Thread() {
			public void run() {
				System.out.println("run");
				while (!isStop) {
					
				}
				System.out.println("over");
			}
		}.start();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				isStop = true;
				System.out.println("hook");
			}
		});
		System.out.println("2");
	}

}
