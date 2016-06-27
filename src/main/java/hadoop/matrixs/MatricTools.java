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

public class MatricTools {
	public static class Map extends Mapper<LongWritable, Text, Cell, Cell> {
		Cell k = new Cell();
		Cell v = new Cell();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Cell, Cell>.Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String[] arr = str.split(" ");
			try {
				if (arr != null && arr.length == 3) {
					int row = Integer.parseInt(arr[0]);
					int col = Integer.parseInt(arr[1]);
					int vl = Integer.parseInt(arr[2]);
					v.setRow(row);
					v.setCol(col);
					v.setValue(vl);
					k.setRow(row);
					k.setCol(col);
					context.write(k, v);
				}
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
		}
	}

	public static class Reduce extends Reducer<Cell, Cell, Text, Cell> {

		Text t = new Text();

		@Override
		protected void reduce(Cell arg0, Iterable<Cell> arg1, Reducer<Cell, Cell, Text, Cell>.Context arg2)
				throws IOException, InterruptedException {
			Iterator<Cell> it = arg1.iterator();
			while (it.hasNext()) {
				Cell c = it.next();
				c.setValue(c.getRow() + c.getCol() + c.getValue());
				t.set(c.toString());
				arg2.write(t, c);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MatricTools.class);
		job.setJobName("Matrix");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Cell.class);
		job.setMapOutputValueClass(Class.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Cell.class);
		job.waitForCompletion(true);
		System.out.println("任务名称：" + job.getJobName());
		System.out.println("任务成功：" + (job.isSuccessful() ? "Yes" : "No"));
	}

}
