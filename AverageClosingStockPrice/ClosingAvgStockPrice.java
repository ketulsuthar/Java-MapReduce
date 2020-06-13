import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClosingAvgStockPrice {

	public static class ClosingAvgPriceMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
		
		private Text stockID = new Text();
		private DoubleWritable closingPrice = new DoubleWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			if(!line.isEmpty()) {
				String[] items = line.split(",");
				stockID.set(items[1]);
				closingPrice.set(Double.parseDouble(items[6]));
				context.write(stockID,closingPrice);
			}
		}
	}
	
	public static class ClosingAvgPriceReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		
		private DoubleWritable avgP = new DoubleWritable();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			double totalPrice = 0.0;
			double avgPrice;
			for(DoubleWritable val : values) {
				count++;
				totalPrice += val.get();
			}
			avgPrice = totalPrice/count;
			avgP.set(avgPrice);
			context.write(key, avgP);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		final String JOBNAME = "AvgClosingStockPrice";
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator","-->");
		Job job = Job.getInstance(conf,JOBNAME);
		
		job.setJarByClass(ClosingAvgStockPrice.class);
		job.setMapperClass(ClosingAvgPriceMapper.class);
		job.setReducerClass(ClosingAvgPriceReducer.class);
		
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
