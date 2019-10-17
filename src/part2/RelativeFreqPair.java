package part2;

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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class RelativeFreqPair {

	public static class PairWritableComparable implements WritableComparable<PairWritableComparable>  {
		// Some data
		public String u;
		public String v;

		public PairWritableComparable() {
		}

		public PairWritableComparable(String u, String v) {
			this.u = u;
			this.v = v;
		}

//		public PairWritable add(PairWritable p) {
//			this.u += p.u;
//			this.v += p.v;
//			return this;
//		}
//
//		public PairWritable add(int sum, int count) {
//			this.u += sum;
//			this.v += count;
//			return this;
//		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(u);
			out.writeUTF(v);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			u = in.readUTF();
			v = in.readUTF();
		}

		public static PairWritableComparable read(DataInput in) throws IOException {
			PairWritableComparable w = new PairWritableComparable();
			w.readFields(in);
			return w;
		}

		@Override
		public String toString( ) {
			return "(" + u + ":" + v + ")";
		}

		@Override
		public int compareTo(PairWritableComparable that) {
			int k = this.u.compareTo(that.u);
			if (k != 0) {
				return k;
			} else {
				return this.v.compareTo(that.v);
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((u == null) ? 0 : u.hashCode());
			result = prime * result + ((v == null) ? 0 : v.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PairWritableComparable other = (PairWritableComparable) obj;
			if (u == null) {
				if (other.u != null)
					return false;
			} else if (!u.equals(other.u))
				return false;
			if (v == null) {
				if (other.v != null)
					return false;
			} else if (!v.equals(other.v))
				return false;
			return true;
		}

    }

	public static class Map extends Mapper<LongWritable, Text, PairWritableComparable, IntWritable> {

		private static List<String> windows(int i, String[] events) {
			List<String> res = new ArrayList<>();
			for (int j = i + 1; j < events.length; ++j) {
				if (events[j].equals(events[i])) break;
				res.add(events[j]);
			}
			return res;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] events = line.split(" +");
			for (int i = 0; i < events.length - 1; ++i) {
				String u = events[i];
				for (String v : windows(i, events)) {
					context.write(new PairWritableComparable(u, v), new IntWritable(1));
					context.write(new PairWritableComparable(u, "*"), new IntWritable(1));
				}
			}

		}
	}

	public static class Partition extends HashPartitioner<PairWritableComparable, IntWritable> {
		@Override
		public int getPartition(PairWritableComparable key, IntWritable value, int numReduceTasks){
			if(numReduceTasks==0)
				return 0;
			return Math.abs(key.u.hashCode()) % numReduceTasks;
		}
	}

	public static class Reduce extends Reducer<PairWritableComparable, IntWritable, PairWritableComparable, DoubleWritable> {

		private int sum;

		@Override
		public void setup(Context context) {
			sum = 0;
		}

		public void reduce(PairWritableComparable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			List<Integer> vInts = new ArrayList<>();
			for(IntWritable vi : values) {
				vInts.add(vi.get());
			}
			System.out.println("[Reducer Input]" + key + ":" + vInts);
			int s = 0;
			for (int val : vInts) {
				s += val;
			}
			if (key.v.equals("*")) {
				sum = s;
			} else {
				context.write(key, new DoubleWritable(s * 1.0 / sum));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "RelativeFreqPair");
		job.setJarByClass(RelativeFreqPair.class);

		job.setMapOutputKeyClass(PairWritableComparable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(PairWritableComparable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		int numReduceTasks = 1;
		if (args.length >= 3) {
			try {
				numReduceTasks = Integer.valueOf(args[2]);
			} catch (NumberFormatException e) {
				System.out.println("Default numReduceTasks:" + numReduceTasks);
			}
		}
		job.setNumReduceTasks(numReduceTasks);

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
