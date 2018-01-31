package mum.cs.bigdata.wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

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
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        HashMap<Text, IntWritable> map = new HashMap<Text, IntWritable>();
	        
	        while (tokenizer.hasMoreTokens()) {
	            word.set(tokenizer.nextToken());
	            
	            if (!map.containsKey(word)) {
	            	map.put(word, one);
	            } else {
	            	IntWritable val = map.get(word);
	            	val.set(val.get() + 1);
	            	map.put(word, val);
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