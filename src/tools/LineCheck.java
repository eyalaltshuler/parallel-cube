package tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class LineCheck extends Configured implements Tool {
		
	public static class TuplesScanner extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		@Override
	    public void map(LongWritable key, Text value, Context context) 
	    		throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
	    }
	}
	
	public static class TuplesReducer extends Reducer<Text, NullWritable, Text, LongWritable> {
		
		@Override 
		public void reduce(Text row, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
			long count = 0;
			for (NullWritable value : values) {
				count++;
				value.toString();
			}
			if (count != 1) {
				context.write(row, new LongWritable(count));
			}
		}
	}
	
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if (args.length != 3) {
			usage();
			return 2;
		}
		Job job = Job.getInstance(getConf());
		Path inputDir = new Path(args[0]);
	    Path outputDir = new Path(args[1]);
	    int reducersNum = Integer.parseInt(args[2]);
	    if (outputDir.getFileSystem(getConf()).exists(outputDir)) {
	      throw new IOException("Output directory " + outputDir + 
	                            " already exists.");
	    }
	    FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setJobName("LinesCount");
	    job.setJarByClass(LineCheck.class);
	    job.setMapperClass(TuplesScanner.class);
	    job.setReducerClass(TuplesReducer.class);
	    job.setNumReduceTasks(reducersNum);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(NullWritable.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	}

	  private void usage() {
		System.err.println("LineCount <in-dir> <out-dir> <reducers-num>");
	}

	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new LineCheck(), args);
	    System.exit(res);
	  }
	
	
}
