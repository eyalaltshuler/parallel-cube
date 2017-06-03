package tools;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.*;

public class AttributesHasher extends Configured implements Tool {
	
	static class AttributeHasherMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
		
		String[] attrs = null;
		
		@Override
		public void setup(Context context) {
			String attrsAsStr = context.getConfiguration().get("attributes");
			attrs = attrsAsStr.split(",");
		}
		
		@Override 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = new String(value.getBytes());
			JSONObject obj = null;
			Text result = null;
			ArrayList<String> data = new ArrayList<String>();
			try {
				obj = new JSONObject(line);
				data.add(String.valueOf(key.get()));
				
				for (int i=0; i<attrs.length; i++) {
					data.add(String.valueOf(obj.getString(attrs[i]).hashCode()));
				}
				data.add("1");
			}
			catch (JSONException e) {
				return;
			}
			result = new Text(StringUtils.join(" ", data));
			context.write(result, NullWritable.get());
		}
	}
			
	@Override
	public int run(String[] args) throws Exception {
			
		System.out.println("Start Generating data...");
		System.out.println("Hashing attribute - " + args[2]);
		
		// round 1 - sample and pre-processing
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
				
		Job job = Job.getInstance(getConf());
		job.setJobName("AttributesHasher");
		job.setJarByClass(AttributesHasher.class);
		job.setMapperClass(AttributeHasherMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.getConfiguration().set("attributes", args[2]);
				
		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		
		System.out.println("MapReduce Round took " + (end - start) + " ms.");
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AttributesHasher(), args);
	    System.exit(res);
	}
}
