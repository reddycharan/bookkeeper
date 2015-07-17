package org.apache.bookkeeper;

import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * The main() loops while listening on the well known port. (5555)
 * Clients connect to the well known port and the client channel is created on accept()
 * A new worker thread is generated for each client channel and that thread ends when client closes the connection
 */
public class BKProxyMain {
	static AtomicInteger threadNum = new AtomicInteger();

	public static void main(String args[]) throws Exception {
		threadNum.set(0);
		ServerSocketChannel serverChannel = ServerSocketChannel.open();
		serverChannel.setOption(java.net.StandardSocketOptions.SO_RCVBUF, 65536);
		serverChannel.socket().bind(new InetSocketAddress(5555));
		SocketChannel sock = null;
//		Place holder for testing with tempLedger;
//		BKtempLedger bk = new BKtempLedger();
		BKSfdcClient bk = new BKSfdcClient();

		while (true) {
			System.out.println("SFStore BK-Proxy: Waiting for connection... ");
			sock = serverChannel.accept();
			
			if (threadNum.get() == BKPConstants.WORKER_THREAD_LIMIT) { // max connections
				System.out.println("Bailing out!! Maximum Connections Reached: " + BKPConstants.WORKER_THREAD_LIMIT);
				break;
			}
			
			// Worker thread to handle each backend's write requests
			new Thread(new BKProxyWorker(threadNum, sock, (Object)bk)).start();

			threadNum.incrementAndGet();
		}
		serverChannel.close();
		System.out.println("Stopping proxy...");
	}
}
