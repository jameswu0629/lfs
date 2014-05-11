package lfs.worker;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * The Server to serve data file read request from remote machines. The current
 * implementation is based on non-blocking NIO.
 */
public class DataServer implements Runnable {
	// private static final Logger LOG =
	// Logger.getLogger(Constants.LOGGER_TYPE);
	private static final Logger LOG = Logger.getLogger(DataServer.class);

	// The host:port combination to listen on
	private InetSocketAddress mAddress;

	// The channel on which we will accept connections
	private ServerSocketChannel mServerChannel;

	// The selector we will be monitoring.
	private Selector mSelector;

	private Map<SocketChannel, DataMessage> mSendingData = Collections
			.synchronizedMap(new HashMap<SocketChannel, DataMessage>());
	private Map<SocketChannel, DataMessage> mReceivingData = Collections
			.synchronizedMap(new HashMap<SocketChannel, DataMessage>());

	// The blocks locker manager.
	// private final BlocksLocker mBlocksLocker;

	private volatile boolean mShutdown = false;
	private volatile boolean mShutdowned = false;
	
	private WorkerInfo workerInfo;

	/**
	 * Create a data server with direct access to worker storage.
	 * 
	 * @param address
	 *            The address of the data server.
	 * @param workerStorage
	 *            The handler of the directly accessed worker storage.
	 */
	public DataServer(InetSocketAddress address, WorkerInfo workerinfo) {
//		LOG.info("Starting DataServer @ " + address);
		mAddress = address;
		this.workerInfo = workerinfo;
		// mBlocksLocker = new BlocksLocker(workerStorage,
		// Users.sDATASERVER_USER_ID);
		try {
			mSelector = initSelector();
		} catch (IOException e) {
			LOG.error(e.getMessage() + mAddress, e);
			// CommonUtils.runtimeException(e);
		}
	}

	private Selector initSelector() throws IOException {
		// Create a new selector
		Selector socketSelector = SelectorProvider.provider().openSelector();

		// Create a new non-blocking server socket channel
		mServerChannel = ServerSocketChannel.open();
		mServerChannel.configureBlocking(false);

		// Bind the server socket to the specified address and port
		mServerChannel.socket().bind(mAddress);

		// Register the server socket channel, indicating an interest in
		// accepting new connections.
		mServerChannel.register(socketSelector, SelectionKey.OP_ACCEPT);

		return socketSelector;
	}

	private void accept(SelectionKey key) throws IOException {
		// For an accept to be pending the channel must be a server socket
		// channel
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key
				.channel();

		// Accept the connection and make it non-blocking
		SocketChannel socketChannel = serverSocketChannel.accept();
		socketChannel.configureBlocking(false);
		
		LOG.info("accept client: " + socketChannel);
		
		// Register the new SocketChannel with our Selector, indicating we'd
		// like to be notified
		// when there is data waiting to be read.
		socketChannel.register(mSelector, SelectionKey.OP_READ);
	}

	private void read(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		DataMessage reqMsg = null;
		
		if (mReceivingData.containsKey(socketChannel)) {	//
			reqMsg = mReceivingData.get(socketChannel);
		} else {
			reqMsg = new DataMessage();
			mReceivingData.put(socketChannel, reqMsg);
		}
		int numRead = 0;
		try {
			numRead = reqMsg.recv(socketChannel);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			key.cancel();
			socketChannel.close();
			mReceivingData.remove(socketChannel);
			mSendingData.remove(socketChannel);
			return;
		}

		if (reqMsg.isReady()) {
			key.interestOps(SelectionKey.OP_WRITE);
			LOG.debug("recv message: " + reqMsg);
			
			reqMsg.data.flip();
			DataMessage respMsg = DataMessage
					.createResponseMessage(reqMsg, this.workerInfo);
			mSendingData.put(socketChannel, respMsg);
		}
	}

	private void write(SelectionKey key) {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		DataMessage respMsg = mSendingData.get(socketChannel);
		boolean ret = false;
		boolean closeChannel = false;
		try {
			ret = respMsg.send(socketChannel);
			LOG.debug("send to " + socketChannel);
		} catch (IOException e) {
			closeChannel = true;
			LOG.error(e.getMessage());
		}
		if (ret || closeChannel) {
			try {
				key.channel().close();
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
			key.cancel();
			mReceivingData.remove(socketChannel);
			mSendingData.remove(socketChannel);
			LOG.debug("send message: " + respMsg);
		}
		
	}
	public void setCloseable(boolean shutdown) {
		mShutdown = shutdown;
	}
	public void close() throws IOException {
		mShutdown = true;
		mServerChannel.close();
		mSelector.close();
	}

	public boolean isClosed() {
		return mShutdowned;
	}

	@Override
	public void run() {
		while (!mShutdown) {
			try {
				// Wait for an event one of the registered channels.
				mSelector.select();
				if (mShutdown) {
					break;
				}

				// Iterate over the set of keys for which events are available
				Iterator<SelectionKey> selectKeys = mSelector.selectedKeys()
						.iterator();
				while (selectKeys.hasNext()) {
					SelectionKey key = (SelectionKey) selectKeys.next();
					selectKeys.remove();

					if (!key.isValid()) {
						continue;
					}

					// Check what event is available and deal with it.
					// TODO These should be multi-thread.
					if (key.isAcceptable()) {
						accept(key);
					} else if (key.isReadable()) {
						read(key);
					} else if (key.isWritable()) {
						write(key);
					}
				}
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				if (mShutdown) {
					break;
				}
				throw new RuntimeException(e);
			}
		}
		mShutdowned = true;
	}
}