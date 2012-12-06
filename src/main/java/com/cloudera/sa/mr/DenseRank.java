package com.cloudera.sa.mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DenseRank {	

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(DenseRank.class);
		job.setJobName("DenseRank");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(DenseRankMapper.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setReducerClass(DenseRankReducer.class);
		job.setNumReduceTasks(1);
		
		job.setSortComparatorClass(ReverseComparator.class);
				
		job.waitForCompletion(true);
	}

	public static class ReverseComparator extends WritableComparator {
		private static LongWritable.Comparator comparator = new LongWritable.Comparator();
		public ReverseComparator() {
			super(LongWritable.class);
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -1 * comparator.compare(b1, s1, l1, b2, s2, l2);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof LongWritable && b instanceof LongWritable) {
				return -1 * (((LongWritable)a).compareTo((LongWritable)b));
			}
			return super.compare(a, b);
		}
		
		
	}
	
	public static class DenseRankMapper extends Mapper<Object, Text, LongWritable, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] valuesArray = value.toString().split("\t");
			Long v = Long.parseLong(valuesArray[valuesArray.length-1]);
			
			context.write(new LongWritable(v), value);
		}		
	}
	
	public static class DenseRankReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
		long rank = 0;
		
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
						
		
			if (values.iterator().hasNext())
				++rank;
			
			for (Text t : values) {
				context.write(t, new LongWritable(rank));
			}			
		}		
		
	}
	
}
