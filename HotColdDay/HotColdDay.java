import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;


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


public class HotColdDay {
	
	public static class HotColdMapper extends Mapper<LongWritable, Text, Text,Text>{
		
		private final float MAX_TEMP = 40;
		private final float MIN_TEMP = 10;
		private final int MISSING = 9999;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			SimpleDateFormat inFormatter = new SimpleDateFormat("yyyymmdd");
			SimpleDateFormat outFormatter = new SimpleDateFormat("mm-dd-yyyy");
			Text text = new Text();
			String line  = value.toString();
			String dayType = "";
			if(!line.isEmpty()) {
				
				float minTemp, maxTemp;
				Date date;
				String inDate = line.substring(6, 14).trim();
				maxTemp = Float.parseFloat(line.substring(39, 45).trim());
				minTemp = Float.parseFloat(line.substring(47, 53).trim());
				try {
					date = inFormatter.parse(inDate);
					String outDate = outFormatter.format(date);
					text.set(outDate);
					
					if (minTemp < MIN_TEMP && minTemp != MISSING) {
						dayType = "Cold Day";
					}
			        
					if (maxTemp > MAX_TEMP && maxTemp != MISSING) {
						dayType = "Hot Day";
					}
					
					if (!dayType.isEmpty())
						context.write(text, new Text(dayType));
					
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		} 
	}
	
	public static class HotColdReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
	}
	
	public static void main(String[] args) throws Exception {
		
		final String JOBNAME = "HotandCodDay";
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf,JOBNAME);
		
		job.setJarByClass(HotColdDay.class);
		job.setMapperClass(HotColdMapper.class);
		
		job.setReducerClass(HotColdReducer.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
