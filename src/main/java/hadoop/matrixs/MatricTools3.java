package hadoop.matrixs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatricTools3 {

	public static final int A_LENGHT = 3;
	public static final int B_LENGHT = 3;

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String[] arr = str.split(" ");
			Text k = new Text();
			Text v = new Text();

			try {
				if (arr != null && arr.length == 3) {
					for (int i = 0; i < B_LENGHT; i++) {
						String key0 = arr[0] + "," + arr[1];
						String value0 = "A_" + arr[0] + "," + arr[1] + "," + arr[2];
						k.set(key0);
						v.set(value0);
						context.write(k, v);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
//			k.set(str+"-"+ arr.length);
//			v.set("asdfasdfa");
//			context.write(k, v);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		Text t = new Text();
		Text t1 = new Text();
		
		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuffer sb = new StringBuffer();
			sb.append(arg0.toString());
//			sb.append("--");
			Iterator<Text> it = arg1.iterator();
			
			while(it.hasNext()){
				Text  t = it.next();
				sb.append(t.toString()+"\t");
				
			}			
			t.set(sb.toString());
			t1.set("haha");
			arg2.write(t,t1);
		}



	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MatricTools3.class);
		job.setJobName("Matrix");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		System.out.println("任务名称：" + job.getJobName());
		System.out.println("任务成功：" + (job.isSuccessful() ? "Yes" : "No"));
	}

}
