package hadoop.mr.ip;

public class LogHelper {
	
	public LogInfo proceed(String line){
		LogInfo li = new LogInfo();
		
		String [] arr = line.split(" ");
		
		if(arr!=null&&arr.length>0){
			System.out.println(arr.length);
			for(String temp : arr){
				System.out.println(temp.trim());
			}
		}
		return li;
	}

}
