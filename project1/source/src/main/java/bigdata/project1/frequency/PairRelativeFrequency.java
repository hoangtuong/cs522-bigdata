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

import bigdata.project1.utils.Pair;
import bigdata.project1.utils.StringPairWritable;

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
		private final static String STAR = "*";
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
					putValue(H, new Pair<String, String>(w, STAR), 1);
					putValue(H, new Pair<String, String>(w, u), 1);
					j++;
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<Pair<String, String>, Integer> entry : H.entrySet()) {
				Pair<String, String> p = entry.getKey();
				context.write(new StringPairWritable(p.getLeft(), p.getRight()), new IntWritable(entry.getValue()));
			}
		}
		
		private void putValue(HashMap<Pair<String, String>, Integer> map, Pair<String, String> key, int value) {
			if (!map.containsKey(key)) {
				map.put(key, value);
			} else {
				map.put(key, map.get(key) + 1);
			}
		}
	}

	public static class Reduce extends Reducer<StringPairWritable, IntWritable, StringPairWritable, DoubleWritable> {
		private final static String STAR = "*";
		private int total = 0;

		@Override
		public void reduce(StringPairWritable pair, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}

			if (pair.getRight().equals(STAR)) {
				total = sum;
			} else {
				context.write(pair, new DoubleWritable((float) sum/total));
			}
		}
	}
}