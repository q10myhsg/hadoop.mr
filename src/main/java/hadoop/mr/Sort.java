package hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Sort {
	
	public static class SortMapper extends Mapper<LongWritable, Text, DataBean, NullWritable>{
		private DataBean db = new DataBean();
		
		

		@Override
		protected void cleanup(Mapper<LongWritable, Text, DataBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}



		@Override
		public void run(Mapper<LongWritable, Text, DataBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			super.run(context);
		}



		@Override
		protected void setup(Mapper<LongWritable, Text, DataBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}



		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, DataBean, NullWritable>.Context context)
						throws IOException, InterruptedException {
//			String line = value.toString();
			
			context.write(db, NullWritable.get());
		}		
	}
	
	public static class SortReducer extends Reducer<DataBean, NullWritable, Text, DataBean>{

		@Override
		protected void reduce(DataBean arg0, Iterable<NullWritable> arg1,
				Reducer<DataBean, NullWritable, Text, DataBean>.Context arg2) throws IOException, InterruptedException {
			super.reduce(arg0, arg1, arg2);
		}		
	}
	public static void main(String[] args){
		
	}
}
