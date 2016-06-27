package hadoop.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataBean implements Writable,Comparable<DataBean> {
	
	public DataBean(){
		
	}
	
	private String telNumber="****";
	private long  upload=0;
	private long download=0;
	private long totalSize=0;
	
	

	//deserialize	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.telNumber=in.readUTF();
		this.upload=in.readLong();
		this.download=in.readLong();
		this.totalSize=in.readLong();
	}
	
	//serialize
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(telNumber);
		out.writeLong(upload);
		out.writeLong(download);
		out.writeLong(totalSize);			
	}
	
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		
		return telNumber+"\t"+upload+"\t"+download+"\t"+totalSize;
	}


	public String getTelNumber() {
		return telNumber;
	}
	public void setTelNumber(String telNumber) {
		this.telNumber = telNumber;
	}
	public long getUpload() {
		return upload;
	}
	public void setUpload(long upload) {
		this.upload = upload;
	}
	public long getDownload() {
		return download;
	}
	public void setDownload(long download) {
		this.download = download;
	}
	public long getTotalSize() {
		return totalSize;
	}
	public void setTotalSize(long totalSize) {
		this.totalSize = totalSize;
	}


	@Override
	public int compareTo(DataBean o) {
		// TODO Auto-generated method stub
		if(this.download>o.download) return 1;
		return 0;
	}
	
}