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

public class LowPriceStock {

	public static class LowPriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		
		private Text stock = new Text();
		private DoubleWritable lowPrice = new DoubleWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			if(!line.isEmpty()) {
				
				String[] items = line.split(",");
				
				stock.set(items[1]);
				lowPrice.set(Double.parseDouble(items[5]));
				
				context.write(stock,lowPrice);
			}
		}
	}
	
	public static class LowPriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		
		private DoubleWritable lowPrice = new DoubleWritable();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			double low = 1000.0;
			for(DoubleWritable val : values) {
				if(low > val.get()) {
					low = val.get();
				}
			}
			lowPrice.set(low);;
			context.write(key,lowPrice);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		final String JOBNAME = "LowPrice";
		Configuration config = new Configuration();
		Job job = Job.getInstance(config,JOBNAME);
		
		job.setJarByClass(LowPriceStock.class);
		
		job.setMapperClass(LowPriceMapper.class);
		job.setReducerClass(LowPriceReducer.class);
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
