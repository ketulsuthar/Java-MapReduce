import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HighStockPrice {
	
	public static class HighPriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		
		private Text stock = new Text();
		private DoubleWritable highPrice = new DoubleWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			if(!line.isEmpty()) {
				
				String[] items = line.split(",");
				
				stock.set(items[1]);
				highPrice.set(Double.parseDouble(items[4]));
				
				context.write(stock,highPrice);
			}
		}
	}
	
	public static class HighPriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		
		private DoubleWritable highPrice = new DoubleWritable();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			double high = 0.0;
			for(DoubleWritable val : values) {
				if(high < val.get()) {
					high = val.get();
				}
			}
			highPrice.set(high);;
			context.write(key,highPrice);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		final String JOBNAME = "AllTimeHighStockPrice";
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator",":");
		Job job = Job.getInstance(conf,JOBNAME);
		
		job.setJarByClass(HighStockPrice.class);
		job.setMapperClass(HighPriceMapper.class);
		job.setReducerClass(HighPriceReducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);

	}

}
