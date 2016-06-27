package hadoop.mr.ip;

public class LogInfo {
	private String ipAddress;
	private String type;
	private String size;
	private String url;
	private String ua;
	public String getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getSize() {
		return size;
	}
	public void setSize(String size) {
		this.size = size;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getUa() {
		return ua;
	}
	public void setUa(String ua) {
		this.ua = ua;
	}
	@Override
	public String toString() {
		return "LogInfo [ipAddress=" + ipAddress + ", type=" + type + ", size=" + size + ", url=" + url + ", ua=" + ua
				+ ", toString()=" + super.toString() + "]";
	}
}
