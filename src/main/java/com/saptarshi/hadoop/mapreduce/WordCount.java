package com.saptarshi.hadoop.mapreduce;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private boolean caseSensitive;
		private boolean skipPatterns;
		private Set<String> patternsToSkip = new HashSet<String>();
		private Configuration conf;

	    private void parseSkipFile(String fileName) throws IOException {
	    	try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
	    		String pattern = null;
	    		while ((pattern = br.readLine()) != null) {
	    			patternsToSkip.add(pattern);
	    		}
	    	}
	    }

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();

			caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
			skipPatterns = conf.getBoolean("wordcount.skip.patterns", false);

			if (skipPatterns) {
		          URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
		          for (URI patternsURI: patternsURIs) {
		        	  Path patternsPath = new Path(patternsURI.getPath());
		        	  String patternsFileName = patternsPath.getName().toString();
		        	  parseSkipFile(patternsFileName);
		          }
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

			if (skipPatterns) {
				for (String pattern: patternsToSkip) {
					line = line.replaceAll(pattern, "");
				}				
			}

			StringTokenizer itr = new StringTokenizer(line);

			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

 		@Override
 		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
 			int sum = 0;
 			for (IntWritable val : values) {
 				sum += val.get();
 			}

 			result.set(sum);
 			context.write(key, result);
 		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();

	    if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
	    	System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
	        System.exit(2);
	    }

		Job job = Job.getInstance(conf, "WordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		job.setCombinerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		List<String> otherArgs = new ArrayList<String>();
		for (int i = 0; i < remainingArgs.length; ++i) {
			if ("-skip".equals(remainingArgs[i])) {
				job.addCacheFile(new Path(remainingArgs[++i]).toUri());
		        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
			}
			else {
				otherArgs.add(remainingArgs[i]);
			}
		}

		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
