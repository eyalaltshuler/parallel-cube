package cube;

import infra.CubeGroup;

import infra.RowOutputFormat;

import java.io.IOException;
//import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ParallelCube extends Configured implements Tool {
	
	private static void usage() throws IOException {
		System.err.println("Cube <input> <output-round1> <output-round2> <probability> <skews-threshold> <dimensions>");
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 7) {
			usage();
			return -1;
		}
		
		System.out.println("Start Computing Cube");
		
		// round 1 - sample and pre-processing
		
		Path input = new Path(args[0]);
		Path roundOneOutput = new Path(args[1]);
		Path roundTwoOutput = new Path(args[2]);
		String probAsStr = args[3];
		String thresholdAsStr = args[4];
		String bucketsNumAsStr = args[5];
		String dimsAsStr = args[6];
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(ParallelCube.class);
		job.setJobName("Cube1");
		job.setMapperClass(CubeRoundOneMapper.class);
		job.setReducerClass(CubeRoundOneReducer.class);
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(new Path("sample"), true);
		fs.delete(new Path("annotatedLattice"), true);
		Configuration fsConf = fs.getConf();
		String dfsBlockSizeAsStr = fsConf.get("dfs.blocksize");
		Long dfsBlockSize = Long.parseLong(dfsBlockSizeAsStr);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, roundOneOutput);
		job.setOutputFormatClass(TextOutputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, dfsBlockSize / 3);
		job.setInputFormatClass(CombineTextInputFormat.class);
		job.getConfiguration().set("sample.probability", probAsStr);
		job.getConfiguration().set("dims", dimsAsStr);
		job.getConfiguration().set("threshold", thresholdAsStr);
		job.getConfiguration().set("buckets",bucketsNumAsStr);
		
		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		
		System.out.println("MapReduce Round 1 took " + (end - start) + " ms.");
		System.out.println("end of round 1");
		
		// round 2 - parallel cube computation
		
		job = Job.getInstance(getConf());
		job.setJobName("Cube2");
		job.setJarByClass(ParallelCube.class);
		job.setMapperClass(CubeRoundTwoMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setReducerClass(CubeRoundTwoReducer.class);
		job.setOutputKeyClass(CubeGroup.class);
		job.setOutputValueClass(NullWritable.class);
		job.setPartitionerClass(CubeRoundTwoPartitioner.class);
		job.setNumReduceTasks(1+Integer.parseInt(bucketsNumAsStr));
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, roundTwoOutput);
		fs = FileSystem.get(getConf());
		fsConf = fs.getConf();
		dfsBlockSizeAsStr = fsConf.get("dfs.blocksize");
		dfsBlockSize = Long.parseLong(dfsBlockSizeAsStr);
		CombineTextInputFormat.setMaxInputSplitSize(job, dfsBlockSize / 3);
		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setOutputFormatClass(RowOutputFormat.class);
//		URI partitionFile = new URI(sample.toString() + "#" + "part-r-00000");
//		job.addCacheFile(partitionFile);
		//job.getConfiguration().set("table.colsnum", "5");
		job.getConfiguration().set("dimensionsOrder",dimsAsStr);
		
		start = System.currentTimeMillis();
		job.waitForCompletion(true);
		end = System.currentTimeMillis();
		
		System.out.println("MapReduce Round 2 took " + (end - start) + " ms.");
		System.out.println("end of round 2");
		System.out.println("Cube computation finished.");
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ParallelCube(), args);
	    System.exit(res);
	}
}
