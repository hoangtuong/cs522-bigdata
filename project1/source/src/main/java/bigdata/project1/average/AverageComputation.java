package mum.cs.bigdata.average;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class AverageComputation extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new AverageComputation(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "wordcount");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private Text word = new Text();
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] strs = line.split(" ");
	        
	        if (strs.length > 1) {
	        	String ipString = strs[0];
	        	String capacity = strs[strs.length-1];
	        	
	        	System.out.println("IP: " + ipString + ": " + capacity);
	        	
	        	try {
	        		int number = Integer.parseInt(capacity);
		        	word.set(ipString);
		        	context.write(word, new IntWritable(number));

	        	} catch(NumberFormatException ex) {
	        		
	        	}
	        }
	    }
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int cnt = 0;
			
			for (IntWritable count : counts) {
				sum += count.get();
				cnt ++;
			}
			context.write(word, new DoubleWritable(sum/(cnt*1.0)));
		}
	}
}