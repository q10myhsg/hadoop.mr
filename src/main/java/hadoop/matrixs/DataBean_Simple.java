package hadoop.matrixs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataBean_Simple implements Writable {
	
	public DataBean_Simple(){
		telNumber="dfs";
		upload=0;
		download=0;
		totalSize=0;
	}
	
	private String telNumber;
	private int  upload;
	private int download;
	private int totalSize;
	
	//deserialize	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upload=in.readInt();
		this.download=in.readInt();
		this.totalSize=in.readInt();
	}
	
	//serialize
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(upload);
		out.writeInt(download);
		out.writeInt(totalSize);			
	}
	
	
	


	@Override
	public String toString() {
		return telNumber+"\t"+upload+"\t"+download+"\t"+totalSize;
	}


	public String getTelNumber() {
		return telNumber;
	}
	public void setTelNumber(String telNumber) {
		this.telNumber = telNumber;
	}
	public int getUpload() {
		return upload;
	}
	public void setUpload(int upload) {
		this.upload = upload;
	}
	public int getDownload() {
		return download;
	}
	public void setDownload(int download) {
		this.download = download;
	}
	public int getTotalSize() {
		return totalSize;
	}
	public void setTotalSize(int totalSize) {
		this.totalSize = totalSize;
	}

}