package mum.cs.bigdata.frequency;

import java.io.IOException;
import java.util.Map.Entry;

import mum.cs.bigdata.utils.StringDoubleMapWritable;
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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class HybridRelativeFrequency extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new HybridRelativeFrequency(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "wordcount");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(StringPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringDoubleMapWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, StringPairWritable, IntWritable> {
	    private final static IntWritable ONE = new IntWritable(1);
	    private MapWritable map = new MapWritable();
	    
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
		        		//context.write(new StringPairWritable(w, u), ONE);
		        		StringPairWritable pair = new StringPairWritable(w, u);
		        		if (!map.containsKey(pair)) {
		        			map.put(pair, ONE);
		        		} else {
		        			int val = ((IntWritable) map.get(pair)).get() + 1;
		        			map.put(pair, new IntWritable(val));
		        		}
		        	}
	        		j ++;
	        	}
	        }
	    }
	    
	    @Override
	    public void cleanup(Context context) throws IOException, InterruptedException {
	    	for (Writable key : map.keySet()) {
	    		StringPairWritable pair = (StringPairWritable) key;
	    		context.write(pair, (IntWritable) map.get(pair));
	    	}
	    }
	}
	
	public static class Reduce extends Reducer<StringPairWritable, IntWritable, Text, StringDoubleMapWritable> {
	    private Text prev = null;
	    private StringDoubleMapWritable map = new StringDoubleMapWritable();
	    
		@Override
		public void reduce(StringPairWritable pair, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			
			Text w = new Text(pair.getLeft());
			Text u = new Text(pair.getRight());
			
			if (prev != null & !w.equals(prev)) {
				// Calculate total on map
				int total = 0;
				for (Entry<Writable, Writable> entry : map.entrySet()) {
					DoubleWritable val = (DoubleWritable) map.get(entry.getKey());
					total += val.get();
				}
				
				// Calculate average
				for (Entry<Writable, Writable> entry : map.entrySet()) {
					double val = ((DoubleWritable) entry.getValue()).get();
					map.put(entry.getKey(), new DoubleWritable(val*1.0/total));
				}
				
				context.write(prev, map);
				map.clear();
			}
			
			// Calculate sum
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			
			map.put(u, new DoubleWritable(sum));
			prev = w;
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			// Calculate total on map
			int total = 0;
			for (Entry<Writable, Writable> entry : map.entrySet()) {
				DoubleWritable val = (DoubleWritable) map.get(entry.getKey());
				total += val.get();
			}
			
			// Calculate average
			for (Entry<Writable, Writable> entry : map.entrySet()) {
				double val = ((DoubleWritable) entry.getValue()).get();
				map.put(entry.getKey(), new DoubleWritable(val*1.0/total));
			}
			
			context.write(prev, map);
		}
	}
}