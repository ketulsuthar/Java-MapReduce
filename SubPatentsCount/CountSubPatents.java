import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountSubPatents {

	public static class SubPatentMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		private Text id = new Text();
		//private Text val = new Text();
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			
			if(!line.isEmpty()) {
				
				StringTokenizer st = new StringTokenizer(line);
				while(st.hasMoreTokens()) {
					id.set(st.nextToken());
					st.nextToken();
				}
				//String[] items = line.split(" ");
				//id.set(Integer.parseInt(items[0]));
				//val.set(items[1]);
				context.write(id, one);
			}
		}	
	}
	
	public static class SubPatentReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
	
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
		final String JOBNAME = "SubPatern";
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf,JOBNAME);
		
		job.setJarByClass(CountSubPatents.class);
		job.setMapperClass(SubPatentMapper.class);
		job.setReducerClass(SubPatentReducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
		
	}

}
