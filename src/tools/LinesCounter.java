package tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LinesCounter extends Configured implements Tool {
		
	public static class LinesCounterMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		
		@Override
	    public void map(LongWritable key, Text value, Context context) 
	    		throws IOException, InterruptedException {
			context.write(new LongWritable(1), new LongWritable(1));
	    }
	}
	
	public static class LinesCounterReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		
		@Override 
		public void reduce(LongWritable row, Iterable<LongWritable> values, Context context) 
				throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable value : values) {
				count+=value.get();
			}
			context.write(new LongWritable(1), new LongWritable(count));
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
	    job.setMapperClass(LinesCounterMapper.class);
	    job.setReducerClass(LinesCounterReducer.class);
	    job.setCombinerClass(LinesCounterReducer.class);
	    job.setNumReduceTasks(reducersNum);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    FileSystem fs = FileSystem.get(getConf());
		Configuration fsConf = fs.getConf();
		String dfsBlockSizeAsStr = fsConf.get("dfs.blocksize");
		long dfsBlockSize = Long.parseLong(dfsBlockSizeAsStr);
		CombineTextInputFormat.setMaxInputSplitSize(job, dfsBlockSize);
		job.setInputFormatClass(CombineTextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	}

	  private void usage() {
		System.err.println("LineCount <in-dir> <out-dir> <reducers-num>");
	}

	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new LinesCounter(), args);
	    System.exit(res);
	  }
	
	
}
