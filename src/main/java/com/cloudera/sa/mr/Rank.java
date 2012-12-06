package com.cloudera.sa.mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Rank extends DenseRank {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(Rank.class);
		job.setJobName("Rank");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(DenseRankMapper.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setReducerClass(RankReducer.class);
		job.setNumReduceTasks(1);
		
		job.setSortComparatorClass(ReverseComparator.class);
				
		job.waitForCompletion(true);
	}	


	public static class RankReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
		long rank = 1;
		int skip = 0;
		
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
						
			if (values.iterator().hasNext()) {
				rank = ++rank + skip - 1;				
				skip = 0;
			}
			
			for (Text t : values) {
				context.write(t, new LongWritable(rank));
				skip++;
			}			
			
		}		
		
	}
}
