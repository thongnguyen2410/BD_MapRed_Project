package part1.f;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InMapperApacheLogAvg {

	public static class PairWritable implements Writable {
		// Some data
		public int sum;
		public int count;

		public PairWritable() {
		}

		public PairWritable(int sum, int count) {
			this.sum = sum;
			this.count = count;
		}

		public PairWritable add(PairWritable p) {
			this.sum += p.sum;
			this.count += p.count;
			return this;
		}

		public PairWritable add(int sum, int count) {
			this.sum += sum;
			this.count += count;
			return this;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(sum);
			out.writeInt(count);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			sum = in.readInt();
			count = in.readInt();
		}

		public static PairWritable read(DataInput in) throws IOException {
			PairWritable w = new PairWritable();
			w.readFields(in);
			return w;
		}

		@Override
		public String toString( ) {
			return "<" + sum + ":" + count + ">";
		}
    }

	public static class Map extends Mapper<LongWritable, Text, Text, PairWritable> {

		private HashMap<Text, PairWritable> H;

		@Override
		public void setup(Context context) {
			H = new HashMap<>();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" +");
			String first = words[0];
			String last = words[words.length - 1];
			//System.out.println(first + ":" + last);
			try {
				Text firstText = new Text(first);
				int lastInt = Integer.valueOf(last);
				if (H.containsKey(firstText)) {
					H.put(firstText, H.get(firstText).add(lastInt, 1));
				} else {
					H.put(firstText, new PairWritable(lastInt, 1));
				}
			} catch (NumberFormatException e) {
				System.out.println(e + ":" + last + " is not number!");
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Text key: H.keySet()) {
				context.write(key, H.get(key));
				//System.out.println(key.toString() + ":" + H.get(key));
			}
		}
	}

	public static class Reduce extends Reducer<Text, PairWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<PairWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (PairWritable val : values) {
				//System.out.println(key.toString() + ":" + sum + ":" + count);
				sum += val.sum;
				count += val.count;
			}
			context.write(key, new DoubleWritable(sum * 1.0 / count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "ApacheLogAvg");
		job.setJarByClass(InMapperApacheLogAvg.class);


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PairWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path output = new Path(args[1]);
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			// true stands for recursively deleting the folder you gave
			fs.delete(output, true);
		}

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}

}
