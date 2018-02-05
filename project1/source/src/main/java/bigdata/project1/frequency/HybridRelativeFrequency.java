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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import bigdata.project1.utils.Pair;
import bigdata.project1.utils.StringDoubleMapWritable;
import bigdata.project1.utils.StringPairWritable;

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
		private HashMap<Pair<String, String>, Integer> H = new HashMap<Pair<String, String>, Integer>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			List<String> wordList = Arrays.asList(line.split(" "));
			wordList.removeAll(Arrays.asList(""));
			String[] words = wordList.toArray(new String[0]);
			String w, u;
			
			for (int i = 0; i < words.length; i++) {
				w = words[i];
				int j = i + 1;
				while (j < words.length && !words[j].equals(words[i])) {
					u = words[j];
					Pair<String, String> pair = new Pair<String, String>(w, u);
					if (!H.containsKey(pair)) {
						H.put(pair, 1);
					} else {
						H.put(pair, H.get(pair)+1);
					}
					j++;
				}
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Pair<String, String> key : H.keySet()) {
				StringPairWritable pair = new StringPairWritable(key.getLeft(), key.getRight());
				context.write(pair, new IntWritable(H.get(key)));
			}
		}
	}

	public static class Reduce extends Reducer<StringPairWritable, IntWritable, Text, StringDoubleMapWritable> {
		private Text prev = null;
		private StringDoubleMapWritable map = new StringDoubleMapWritable();

		@Override
		public void reduce(StringPairWritable pair, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
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
					map.put(entry.getKey(), new DoubleWritable((float)val/total));
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

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			// Calculate total on map
			int total = 0;
			for (Entry<Writable, Writable> entry : map.entrySet()) {
				DoubleWritable val = (DoubleWritable) map.get(entry.getKey());
				total += val.get();
			}

			// Calculate average
			for (Entry<Writable, Writable> entry : map.entrySet()) {
				double val = ((DoubleWritable) entry.getValue()).get();
				map.put(entry.getKey(), new DoubleWritable((float) val/total));
			}

			context.write(prev, map);
		}
	}
}