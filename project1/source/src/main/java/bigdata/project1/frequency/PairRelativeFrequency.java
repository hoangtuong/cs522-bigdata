package mum.cs.bigdata.frequency;

import java.io.IOException;

import mum.cs.bigdata.utils.StringPairWritable;

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

public class PairRelativeFrequency extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new PairRelativeFrequency(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "wordcount");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(StringPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, StringPairWritable, IntWritable> {
	    private final static IntWritable ONE = new IntWritable(1);
	    private final static String STAR = "*";
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] words = line.split(" ");
	        
	        String w;
	        String u;
	        for (int i = 0; i < words.length; i ++) {
	        	w = words[i];
	        	int j = i+1;
	        	while (j < words.length && !words[j].equals(words[i]) ) {
	        		u = words[j];
		        	if (u.trim().length() > 0 && w.trim().length() > 0 && !u.equals(w)) {
		        		context.write(new StringPairWritable(w, STAR), ONE);
		        		context.write(new StringPairWritable(w, u), ONE);
		        	}
	        		j ++;
	        	}
	        }
	    }
	}
	
	public static class Reduce extends Reducer<StringPairWritable, IntWritable, StringPairWritable, DoubleWritable> {
	    private final static String STAR = "*";
	    private int total = 0;
	    
		@Override
		public void reduce(StringPairWritable pair, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			
			if (pair.getRight().equals(STAR)) {
				total = sum;
			} else {
				context.write(pair, new DoubleWritable(sum*1.0/total));		
			}
		}
	}
}