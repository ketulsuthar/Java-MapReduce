import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTempratureByYear {

	public static class TempratureMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
		
		private IntWritable temp = new IntWritable();
		private IntWritable year = new IntWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
			String line = value.toString();
			if(!line.isEmpty()) {
				
				String[] items = line.toString().split(" ");
				temp.set(Integer.parseInt(items[1]));
				year.set(Integer.parseInt(items[0]));
				context.write(year, temp);
			}
		}
	}
	
	public static class TempratureReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
			
			int maxTemp = 0;
			for(IntWritable temp : values) {
				
				if(temp.get() > maxTemp) {
					maxTemp = temp.get();
				}
			}
			context.write(key, new IntWritable(maxTemp));
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		final String jobName = "HighTemprature";
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf,jobName);
		
		job.setJarByClass(MaxTempratureByYear.class);
		
		job.setMapperClass(TempratureMapper.class);
		job.setReducerClass(TempratureReducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);

	}

}
