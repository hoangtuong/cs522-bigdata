package mum.cs.bigdata.wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class InMapperWordCount extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new InMapperWordCount(), args);
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
		private final static IntWritable ONE = new IntWritable(1);
		// Regular expression to find a valid word
	    private static String WORD_REGEX = "\\b[^\\d\\r\\n|\\r_ -]+\\b";

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        Pattern pattern = Pattern.compile(WORD_REGEX);
	        Matcher matcher = pattern.matcher(line);
	        HashMap<Text, IntWritable> map = new HashMap<Text, IntWritable>();
	        
	        while (matcher.find()) {
	            String w = matcher.group().toLowerCase();

	            if (!w.contains(".")) {
	            	 Text word = new Text(w);
	                if (!map.containsKey(word)) {
		            	map.put(word, ONE);
		            } else {
		            	int val = map.get(word).get();
		            	map.put(word, new IntWritable(val+1));
		            }
	            }
	        }
	        
	        for (Entry<Text, IntWritable> entry : map.entrySet()) {
            	context.write(entry.getKey(), entry.getValue());
	        }
	    }
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}
}