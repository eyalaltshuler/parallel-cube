package tools;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SubSampler extends Configured implements Tool {
	
	public static class SubSamplingMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private double prob = 0;
		private Random rand = null;
		
		@Override
		public void setup(Context context) {
			prob = Double.parseDouble(context.getConfiguration().get("sample.probability"));
			rand = new Random();
		}
		
		@Override
	    public void map(LongWritable key, Text value, Context context) 
	    		throws IOException, InterruptedException {
			double x = rand.nextDouble();
			if (x<=prob) {
				context.write(value,NullWritable.get());
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
	    String probAsStr = args[2];
	    if (outputDir.getFileSystem(getConf()).exists(outputDir)) {
	      throw new IOException("Output directory " + outputDir + 
	                            " already exists.");
	    }
	    FileSystem fs = FileSystem.get(getConf());
		Configuration fsConf = fs.getConf();
		String dfsBlockSizeAsStr = fsConf.get("dfs.blocksize");
		Long dfsBlockSize = Long.parseLong(dfsBlockSizeAsStr);
	    FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setJobName("SubSampler");
	    job.setJarByClass(SubSampler.class);
	    job.setMapperClass(SubSamplingMapper.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    CombineTextInputFormat.setMaxInputSplitSize(job, dfsBlockSize);
		job.setInputFormatClass(CombineTextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(NullWritable.class);
	    job.getConfiguration().set("sample.probability", probAsStr);
	    return job.waitForCompletion(true) ? 0 : 1;
	}

	  private void usage() {
		System.err.println("LineCount <in-dir> <out-dir> <reducers-num>");
	}

	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new SubSampler(), args);
	    System.exit(res);
	  }
	
	
}
