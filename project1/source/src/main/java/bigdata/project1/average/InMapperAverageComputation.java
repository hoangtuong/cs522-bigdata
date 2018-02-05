package bigdata.project1.average;

import java.io.IOException;
import java.util.HashMap;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import bigdata.project1.utils.IntPairWritable;
import bigdata.project1.utils.Pair;

public class InMapperAverageComputation extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new InMapperAverageComputation(), args);
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
		job.setMapOutputValueClass(IntPairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntPairWritable> {
		private HashMap<String, Pair<Integer, Integer>> H = new HashMap<String, Pair<Integer, Integer>>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] strs = line.split(" ");

			if (strs.length < 1) return;
			
			String ipString = strs[0];
			String capacity = strs[strs.length - 1];

			try {
				int number = Integer.parseInt(capacity);
				if (!H.containsKey(ipString)) {
					H.put(ipString, new Pair<Integer, Integer>(number, 1));
				} else {
					Pair<Integer, Integer> p = H.get(ipString);
					H.put(ipString, new Pair<Integer, Integer>(p.getLeft()+number, p.getRight()+1));
				}
			} catch (NumberFormatException ex) {
			}			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Entry<String, Pair<Integer, Integer>> entry : H.entrySet()) {
				Pair<Integer, Integer> p = entry.getValue();
				context.write(new Text(entry.getKey()), new IntPairWritable(p.getLeft(), p.getRight()));
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntPairWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<IntPairWritable> pairs, Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;

			for (IntPairWritable pair : pairs) {
				sum += pair.getFirst();
				count += pair.getSecond();
			}
			context.write(word, new DoubleWritable((float) sum/count));
		}
	}
}