package hadoop.mr.ip;

public class MainTest {

	public static void main(String[] args) {
		String line = "140.205.253.138 GET / 200 24161 Mozilla 58.217.192.54";
		LogHelper lh = new LogHelper();
		LogInfo li =lh.proceed(line);		
		li.toString();		
	}

}
