 package rollup;

import infra.CubeGroup;
import infra.RowInputFormat;
import infra.RowOutputFormat;
import infra.UniqueRowInputFormat;
import infra.UniqueRowOutputFormat;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cube.CubeRoundOneMapper;

public class RollUp extends Configured implements Tool  {
	
	@Override
	public int run(String[] args) throws Exception {
		
		System.out.println("Start Computing RollUp");
		
		// round 0 - sample and create partitions
		Job job = Job.getInstance(getConf());
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Path sample = new Path("sample");
		job.setJarByClass(RollUp.class);
		job.setJobName("RollUp sampling round");
		job.setMapperClass(CubeRoundOneMapper.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(CubeGroup.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, sample);
		job.setInputFormatClass(UniqueRowInputFormat.class);
		job.setOutputFormatClass(UniqueRowOutputFormat.class);
		job.getConfiguration().set("sample.probability", "0.12");
		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		System.out.println("Spent " + (end - start) + "ms computing partitions.");
		System.out.println("end of round 0");
		
		// round 1
		job = Job.getInstance(getConf());
		job.setJobName("RollUp round 1");
		job.setJarByClass(RollUp.class);
		job.setReducerClass(RollUpRoundOneReducer.class);
		job.setOutputKeyClass(CubeGroup.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.setInputFormatClass(UniqueRowInputFormat.class);
		job.setOutputFormatClass(RowOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, "finalround1", RowOutputFormat.class, CubeGroup.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "partial", RowOutputFormat.class, CubeGroup.class, NullWritable.class);
		URI partitionFile = new URI(sample.toString() + "#" + "part-r-00000");
		job.addCacheFile(partitionFile);
		job.getConfiguration().set("table.colsnum", "5");
		job.getConfiguration().set("dimensionsOrder","0,1,2,3");
		job.getConfiguration().set("reduces-num","1");
		job.setPartitionerClass(RollUpPartitioner.class);
		job.waitForCompletion(true);
		System.out.println("end of round 1");
		
//		// round 2
		job = Job.getInstance(getConf());
		job.setJobName("RollUp round 2");
		job.setJarByClass(RollUp.class);
		job.setMapperClass(RollUpRoundTwoMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(RowInputFormat.class);
		job.setOutputFormatClass(RowOutputFormat.class);
		Path partialFilePath = new Path(output.toString() + "/partial");
		Path partialOutputFilePath = new Path(output.toString() + "/round2");
		FileInputFormat.addInputPath(job, partialFilePath);
		FileOutputFormat.setOutputPath(job, partialOutputFilePath);
		job.setOutputKeyClass(CubeGroup.class);
		job.setOutputValueClass(NullWritable.class);
		job.waitForCompletion(true);
		System.out.println("end of round 2");
		
		System.out.println("Rollup computation finished.");
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RollUp(), args);
	    System.exit(res);
	}
}
