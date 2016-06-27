package hadoop.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCount {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			Job job = Job.getInstance(conf);
			job.setJarByClass(DataCount.class);
			job.setMapperClass(DCMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DataBean.class);
			
			job.setReducerClass(DCReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DataBean.class);
//			job.setPartitionerClass(DCPartitioner.class);
			
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static class DCMapper extends Mapper<LongWritable, Text, Text, DataBean> {

		Text privat_key = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataBean>.Context context)
				throws IOException, InterruptedException {

			System.out.println("mapper key:" + key);
			String[] arr = value.toString().split("\t");
			
			DataBean db = new DataBean();
			if(arr.length==3){
				db.setTelNumber(arr[0]);
				db.setUpload(Long.parseLong(arr[1]));
				db.setDownload(Long.parseLong(arr[2]));
				db.setTotalSize(db.getUpload() + db.getDownload());
				db.setUpload(arr.length);
				privat_key.set(db.getTelNumber());
				context.write(privat_key, db);
//				context.write(privat_key, db);
			}
			
			
		}
	}

	public static class DCReducer extends Reducer<Text, DataBean, Text, DataBean> {
		DataBean db = new DataBean();
		@Override
		protected void reduce(Text text, Iterable<DataBean> dit,
				Reducer<Text, DataBean, Text, DataBean>.Context context) throws IOException, InterruptedException {
			long up = 0;
			long down = 0;
			long total = 0;
			Iterator<DataBean> it = dit.iterator();
			while (it.hasNext()) {
				DataBean db = it.next();
				up += db.getUpload();
				down += db.getDownload();
				total += db.getTotalSize();	
			}
			db.setUpload(up);
			db.setDownload(down);
			db.setTotalSize(total);
			context.write(text, db);
		}
	}
	
	
	public static class DCPartitioner extends  Partitioner<Text, DataBean>{
		
		private static Map<String,Integer> provider = new HashMap<String,Integer>();
		
		static{
			provider.put("138", 1);
			provider.put("139", 1);
			provider.put("152", 2);
			provider.put("153", 2);
			provider.put("182", 3);
			provider.put("183", 3);
		}
		@Override
		public int getPartition(Text key, DataBean value, int numPartitions) {
			//向数据库或配置信息 读写
			String tel_sub = key.toString().substring(0,3);
			Integer count = provider.get(tel_sub);
			if(count == null){
				count = 0;
			}
			return count;
		}		
	}
}
