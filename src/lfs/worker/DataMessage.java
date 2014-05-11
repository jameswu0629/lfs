package lfs.worker;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

public class DataMessage {
	private static final Logger LOG = Logger.getLogger(DataMessage.class);
	public static final byte C_REQ_READ = 1;
	public static final byte C_REQ_WRITE = 2;
	public static final byte S_RSP_READ = 3;
	public static final byte S_RSP_WRITE = 4;
	
	public static final byte NO_ERROR = 0;
	public static final byte READ_ERROR = 1;
	public static final byte WRITE_ERROR = 2;
	
	private static final int HEADER_SIZE = 18;
	
	public ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
	public ByteBuffer data = ByteBuffer.allocate(0);
	
	public byte type;
	public short volumeid;
	public int blockid;
	public short infileid;
	public int offset;	//	希望读取或写的文件偏移量
	public int length;	//	希望读取或写的文件长度（data长度）
	public byte errorCode;	//	错误代码
	
	protected boolean isReady = false;	//true， 消息读取完毕；fasle，未读取完毕
	
	public DataMessage() {
		
	}
	public DataMessage(byte type) {
		this(type, (short)0, 0, (short)0, 0, 0, NO_ERROR);
	}

	public DataMessage(byte type, short volumeid, int blockid, short infileid,
			int offset, int length, byte errorCode) {
		this.type = type;
		this.volumeid = volumeid;
		this.blockid = blockid;
		this.infileid = infileid;
		this.offset = offset;
		this.length = length;
		this.errorCode = errorCode;
	}

	/**
	 * 写数据到管道
	 * @param channel
	 * @return	true, read over; flase, not over.
	 * @throws IOException
	 */
	public boolean send(SocketChannel channel) throws IOException {
		channel.write(this.header);
		if (!this.header.hasRemaining()) {
			channel.write(this.data);
		}
		return !header.hasRemaining() && !data.hasRemaining();
	}
	
	
	/**
	 * 从管道读数据
	 * @param channel
	 * @return
	 * @throws IOException
	 */
	public int recv(SocketChannel channel) throws IOException {
		int numRead = 0;
		if (header.hasRemaining()) {	// 读响应消息头
			numRead = channel.read(header);
			if (!header.hasRemaining()) { // 响应消息头读取完毕
				this.header.flip();
				this.type = header.get();
				this.volumeid = header.getShort();
				this.blockid = header.getInt();
				this.infileid = header.getShort();
				this.offset = header.getInt();
				this.length = header.getInt();
				this.errorCode = header.get();  
				
				if (this.type == DataMessage.S_RSP_READ) {	//读文件的响应消息
					this.data = ByteBuffer.allocateDirect(length);
				} else if (this.type == DataMessage.S_RSP_WRITE){	//	写文件请求的响应消息
					this.isReady = true;
				} else if (this.type == DataMessage.C_REQ_READ) {
					this.isReady = true;
				} else {
					this.data = ByteBuffer.allocateDirect(length);
				}
				//错误的消息格式
			}
		} else {
			numRead = channel.read(data);
			if (!data.hasRemaining()) {
				this.isReady = true;
			}
		}
		return numRead;
	}

	/**
	 * 客户端创建一个读文件的请求消息
	 * @param vid	卷编号
	 * @param bid	块编号
	 * @param fid	文件编号
	 * @param offset	读取偏移量
	 * @param len	读取长度
	 * @return
	 */
	public static DataMessage createReadRequestMessage(short vid, int bid, short fid, int offset, int len) {
		DataMessage reqMsg = new DataMessage(DataMessage.C_REQ_READ,
				vid, bid, fid, offset, len, NO_ERROR);
		reqMsg.generateHeader();
		return reqMsg;
	}

	/**
	 * 客户端创建一个写文件请求的消息
	 * @param buf
	 * @return
	 */
	public static DataMessage createWriteRequestMessage(byte[] buf) {
		DataMessage msg = new DataMessage(DataMessage.C_REQ_WRITE);
		msg.length = buf.length;
		msg.generateHeader();
		msg.data = ByteBuffer.wrap(buf);
		return msg;
	}
	
	/**
	 * 服务器对请求消息req做回复
	 * @param req
	 * @param workerinfo
	 * @return
	 */
	public static DataMessage createResponseMessage(DataMessage req, WorkerInfo workerinfo) {
		DataMessage respMsg = null;
		ByteBuffer buffer = null;
		byte error = NO_ERROR;
		if (req.type == DataMessage.C_REQ_READ) {	//处理读文件请求
			try {
				buffer = workerinfo.fetch(req.blockid, req.infileid, req.offset, req.length);
			} catch (IOException | URISyntaxException e) {
				LOG.error(e.getMessage());
				error = READ_ERROR;
			}
			respMsg = new DataMessage(DataMessage.S_RSP_READ, req.volumeid, 
					req.blockid, req.infileid, req.offset, buffer.capacity(), error);
			respMsg.setData(buffer);
		} else {	//处理写文件请求
			long gid = 0;
			try {
				gid = workerinfo.store2(req.data);
			} catch (IOException e) {
				LOG.error(e.getMessage());
				error = WRITE_ERROR;
			}
			short vid = (short) (gid >>> 48);
			int bid = (int)(gid >>> 16 & 0xFFFFFFFF);
			short fid = (short) (gid & 0xFFFF);
			respMsg = new DataMessage(DataMessage.S_RSP_WRITE, vid, bid, fid, req.offset, req.length, error);
		}
		respMsg.generateHeader();
		return respMsg;
	}
	
	public void setData(ByteBuffer buffer) {
		this.data = buffer;
	}

	private void generateHeader() {
		header.clear();
		header.put(type);
		header.putShort(volumeid);
		header.putInt(blockid);
		header.putShort(infileid);
		header.putInt(offset);
		header.putInt(length);
		header.put(errorCode);
		header.flip();
	}

	public boolean isReady() {
		return isReady;
	}

	public void setReady(boolean isReady) {
		this.isReady = isReady;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("type=").append(type)
			.append(",vid=").append(volumeid)
			.append(",blockid=").append(blockid)
			.append(",infileid=").append(infileid)
			.append(",offset=").append(offset)
			.append(",length=").append(length)
			.append(",header=").append(header)
			.append(",data=").append(data);
		return sb.toString();
	}

}
