package bigdata.project1.frequency;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

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

import bigdata.project1.utils.StringDoubleMapWritable;

public class StripeRelativeFrequency extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new StripeRelativeFrequency(), args);
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
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputValueClass(StringDoubleMapWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        List<String> wordList = Arrays.asList(line.split(" "));
	        wordList.removeAll(Arrays.asList(""));
	        String[] words = wordList.toArray(new String[0]);
	        Text w, u;

	        for (int i = 0; i < words.length; i ++) {
	        	MapWritable H = new MapWritable();	        	
	        	w = new Text(words[i]);
	        	int j = i+1;
	        	
	        	while (j < words.length && !words[j].equals(words[i]) ) {
	        		u = new Text(words[j]);
	        		
	        		if (!words[j].equals(words[i])) {
	        			if (H.containsKey(u)) {
	        				IntWritable val = (IntWritable) H.get(u);
	        				H.put(u, new IntWritable(val.get() + 1));
	        			} else {
	        				H.put(u, one);
	        			}
	        		}
	        		j ++;
	        	}
	        	
	        	if (!H.isEmpty()) {
	        		context.write(new Text(w), H);
	        	}
	        }
	    }	    
	}
	
	public static class Reduce extends Reducer<Text, MapWritable, Text, StringDoubleMapWritable> {
		@Override
		public void reduce(Text word, Iterable<MapWritable> stripes, Context context)
				throws IOException, InterruptedException {
			StringDoubleMapWritable map = new StringDoubleMapWritable();
			
			for (MapWritable stripe : stripes) {
				for (Entry<Writable, Writable> entry : stripe.entrySet()) {
					Text key = (Text) entry.getKey();
					IntWritable value = (IntWritable) entry.getValue();
					
					if (!map.containsKey(key)) {
						map.put(key, value);
					} else {
						int val = ((IntWritable) map.get(key)).get() + value.get();
						map.put(key, new IntWritable(val));
					}					
				}
			}
			
			int sum = 0;

			for (Entry<Writable, Writable> entry : map.entrySet()) {
				IntWritable value = (IntWritable) entry.getValue();
				sum += value.get();
			}
			
			for (Entry<Writable, Writable> entry : map.entrySet()) {
				Text key = (Text) entry.getKey();
				IntWritable value = (IntWritable) entry.getValue();
				
				map.put(key, new DoubleWritable(value.get()*1.0/sum));
			}
			
			context.write(word, map);
		}
	}
}