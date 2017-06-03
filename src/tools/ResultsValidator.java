package tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ResultsValidator extends Configured implements Tool {
		
	public static class TuplesScanner extends Mapper<LongWritable, Text, Text, LongWritable> {
	    
		private LongWritable one = new LongWritable(1);
		
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	      	context.write(value, one);
	    }
	}
	
	public static class TuplesCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		@Override 
		public void reduce(Text row, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			
			context.write(row, new LongWritable(sum));
		}
	}
	
	public static class TuplesReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		@Override 
		public void reduce(Text row, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			if (sum != 2) {
				context.write(row, new LongWritable(sum));
			}
		}
	}
	
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if (args.length != 4) {
			usage();
			return 2;
		}
		
		Job job = Job.getInstance(getConf());
		Path inputDir0 = new Path(args[0]);
		Path inputDir1 = new Path(args[1]);
	    Path outputDir = new Path(args[2]);
	    int reducersNum = Integer.parseInt(args[3]);
	    if (outputDir.getFileSystem(getConf()).exists(outputDir)) {
	      throw new IOException("Output directory " + outputDir + " already exists.");
	    }
	    FileInputFormat.addInputPath(job, inputDir0);
	    FileInputFormat.addInputPath(job, inputDir1);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setJobName("ResultsValidator");
	    job.setJarByClass(ResultsValidator.class);
	    job.setMapperClass(TuplesScanner.class);
	    job.setCombinerClass(TuplesCombiner.class);
	    job.setReducerClass(TuplesReducer.class);
	    job.setNumReduceTasks(reducersNum);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}

	private void usage() {
		System.err.println("ResultsValidator <in-dir1> <in-dir2> <out-dir> <reducers-num>");
		
	}

	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new ResultsValidator(), args);
	    System.exit(res);
	  }
	
	
}
