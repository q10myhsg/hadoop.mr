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


public class MatricTools2 {

	public static class Map extends Mapper<LongWritable, Text, DataBean_Simple, DataBean_Simple> {
		DataBean_Simple k = new DataBean_Simple();
		DataBean_Simple v = new DataBean_Simple();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DataBean_Simple, DataBean_Simple>.Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String[] arr = str.split(" ");
			try {
				if (arr != null && arr.length == 3) {
					int row = Integer.parseInt(arr[0]);
					int col = Integer.parseInt(arr[1]);
					int vl = Integer.parseInt(arr[2]);

					k.setTotalSize(row);
					k.setUpload(col);
					v.setTotalSize(row);
					v.setUpload(col);
					v.setTotalSize(vl);
					context.write(k, v);
				}
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
		}
	}

	public static class Reduce extends Reducer<DataBean_Simple, DataBean_Simple, Text, DataBean_Simple> {

		Text t = new Text();

		@Override
		protected void reduce(DataBean_Simple arg0, Iterable<DataBean_Simple> arg1,
				Reducer<DataBean_Simple, DataBean_Simple, Text, DataBean_Simple>.Context arg2) throws IOException, InterruptedException {
			Iterator<DataBean_Simple> it = arg1.iterator();
			while (it.hasNext()) {
				DataBean_Simple c = it.next();
				c.setTotalSize(c.getTotalSize());
				t.set(c.toString());
				arg2.write(t, c);
			}
		}
	}

	// @Override
	// public int run(String[] arg0) throws Exception {
	// Configuration conf=getConf();
	// Job job =Job.getInstance(conf);
	// job.setJarByClass(MatricTools.class);
	// job.setJobName("Matrix");
	//
	//
	// FileInputFormat.addInputPath(job, new Path("/data/a.txt"));
	// FileOutputFormat.setOutputPath(job, new Path("/data/reslut/"));
	//
	// job.setMapperClass(Map.class);
	// job.setMapOutputKeyClass(Cell.class);
	// job.setMapOutputValueClass(Class.class);
	//
	//
	// job.setReducerClass(Reduce.class);
	// job.setOutputFormatClass(TextOutputFormat.class);
	// job.setOutputKeyClass(Text.class);
	// job.setOutputValueClass(Cell.class);
	// job.waitForCompletion(true);
	// System.out.println("任务名称："+job.getJobName());
	// System.out.println("任务成功："+(job.isSuccessful()?"Yes":"No"));
	// System.out.println("跳过行数："+job.getCounters().findCounter(Counter.LINESKIP).getValue());
	// return job.isSuccessful()?0:1;
	// }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MatricTools2.class);
		job.setJobName("Matrix");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(DataBean_Simple.class);
		job.setMapOutputValueClass(DataBean_Simple.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean_Simple.class);
		job.waitForCompletion(true);
		System.out.println("任务名称：" + job.getJobName());
		System.out.println("任务成功：" + (job.isSuccessful() ? "Yes" : "No"));

	}

}
