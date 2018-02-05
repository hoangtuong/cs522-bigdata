package bigdata.project1.frequency;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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
		private HashMap<String, HashMap<String, Integer>> G = new HashMap<String,HashMap<String, Integer>>();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			List<String> wordList = Arrays.asList(line.split(" "));
			wordList.removeAll(Arrays.asList(""));
			String[] words = wordList.toArray(new String[0]);
			String w, u;
			
			for (int i = 0; i < words.length; i++) {
				HashMap<String, Integer> H = new HashMap<String, Integer>();
				w = words[i];
				int j = i + 1;
				
				while (j < words.length && !words[j].equals(words[i])) {
					u = words[j];
					elementWiseIncrease(H, u);
					j++;
				}
				
				elementWiseSum(G, H, w);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<String, HashMap<String, Integer>> entry : G.entrySet()) {
				HashMap<String, Integer> value = entry.getValue();
				
				MapWritable map = new MapWritable();
				for (Entry<String, Integer> e : value.entrySet()) { 
					map.put(new Text(e.getKey()), new IntWritable(e.getValue()));
				}
				
				context.write(new Text(entry.getKey()), map);
			}
		}
		
		private void elementWiseIncrease(HashMap<String, Integer> map, String key) {
			if (map.containsKey(key)) {
				map.put(key, map.get(key) + 1);
			} else {
				map.put(key, 1);
			}
		}
		
		private void elementWiseSum(HashMap<String, HashMap<String, Integer>> to, HashMap<String, Integer> from, String key) {
			if (!to.containsKey(key)) {
				G.put(key, from);
			} else {
				HashMap<String, Integer> map = to.get(key);
				
				for (Entry<String, Integer> fromEntry : from.entrySet()) {
					String k = fromEntry.getKey();

					if (!map.containsKey(k)) {
						map.put(k, fromEntry.getValue());
					} else {
						map.put(k, fromEntry.getValue() + map.get(k));
					}
				}				
			}
		}
	}

	public static class Reduce extends Reducer<Text, MapWritable, Text, StringDoubleMapWritable> {
		@Override
		public void reduce(Text word, Iterable<MapWritable> stripes, Context context) throws IOException, InterruptedException {
			StringDoubleMapWritable map = new StringDoubleMapWritable();

			for (MapWritable stripe : stripes) {
				for (Entry<Writable, Writable> entry : stripe.entrySet()) {
					Text key = (Text) entry.getKey();
					IntWritable value = (IntWritable) entry.getValue();

					if (!map.containsKey(key)) {
						map.put(key, value);
					} else {
						int val = ((IntWritable) map.get(key)).get()
								+ value.get();
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

				map.put(key, new DoubleWritable(value.get() * 1.0 / sum));
			}

			context.write(word, map);
		}
	}
}