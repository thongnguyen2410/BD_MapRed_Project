package part5;

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

public class FriendFinding {

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }

		@Override
		public String toString() {
			String[] arr = super.toStrings();
			String res  = "[ ";
			for (String s : arr) {
				res += s + ", ";
			}
			return res.substring(0, res.length() - 2) + " ]";
		}
    }

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
			return "(" + u + ", " + v + ")";
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

	public static class Map extends Mapper<LongWritable, Text, PairWritableComparable, TextArrayWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] friends = line.split(" +");
			TextArrayWritable fl = new TextArrayWritable(Arrays.copyOfRange(friends, 1, friends.length));
			String user = friends[0];
			for (int i = 1; i < friends.length; ++i) {
				String friend = friends[i];
				if (user.compareTo(friend) >= 0) {
					context.write(new PairWritableComparable(friend, user), fl);
				} else {
					context.write(new PairWritableComparable(user, friend), fl);
				}
			}
		}
	}

//	public static class Partition extends HashPartitioner<PairWritableComparable, IntWritable> {
//		@Override
//		public int getPartition(PairWritableComparable key, IntWritable value, int numReduceTasks){
//			if(numReduceTasks==0)
//				return 0;
//			return Math.abs(key.u.hashCode()) % numReduceTasks;
//		}
//	}

	public static class Reduce extends Reducer<PairWritableComparable, TextArrayWritable, PairWritableComparable, TextArrayWritable> {

		public void reduce(PairWritableComparable key, Iterable<TextArrayWritable> values, Context context)
				throws IOException, InterruptedException {

			String[] common = null;
			for (TextArrayWritable fl : values) {
				String[] fls = fl.toStrings();
				System.out.println("[Reducer Input]" + key + ":" + fl);
				if (common == null) {
					common = fls;
				}
				Set<String> s1 = new HashSet<String>(Arrays.asList(common));
				Set<String> s2 = new HashSet<String>(Arrays.asList(fls));
				s1.retainAll(s2);
				common = s1.toArray(new String[s1.size()]);
			}

			context.write(key, new TextArrayWritable(common));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "FriendFinding");
		job.setJarByClass(FriendFinding.class);

		//job.setMapOutputKeyClass(PairWritableComparable.class);
		//job.setMapOutputValueClass(ArrayWritable.class);

		job.setOutputKeyClass(PairWritableComparable.class);
		job.setOutputValueClass(TextArrayWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//job.setPartitionerClass(Partition.class);

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
