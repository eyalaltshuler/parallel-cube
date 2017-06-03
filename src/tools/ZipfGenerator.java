package tools;

import infra.Element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.math3.distribution.ZipfDistribution;

import terasort.RangeInputFormat;
import terasort.TeraGen;
import terasort.TeraOutputFormat;

public class ZipfGenerator extends Configured implements Tool {

	public static final String NUM_ROWS = "mapreduce.terasort.num-rows";
	
	private static void usage() throws IOException {
		System.err.println("DataBaseGenerator <num rows> <num cols> <output dir> <reducers num> <number of elements> <exponent>");
	}

	/**
	 * Parse a number that optionally has a postfix that denotes a base.
	 * @param str an string integer with an option base {k,m,b,t}.
	 * @return the expanded value
	*/
	private static long parseHumanLong(String str) {
		char tail = str.charAt(str.length() - 1);
		long base = 1;
		switch (tail) {
		case 't':
			base *= 1000 * 1000 * 1000 * 1000;
			break;
		case 'b':
			base *= 1000 * 1000 * 1000;
			break;
		case 'm':
			base *= 1000 * 1000;
			break;
		case 'k':
			base *= 1000;
			break;	    
		default:
	    }
	    if (base != 1) {
	    	str = str.substring(0, str.length() - 1);
	    }
	    return Long.parseLong(str) * base;
	}
	
	static long getNumberOfRows(JobContext job) {
		return job.getConfiguration().getLong(NUM_ROWS, 0);
	}
		  
	static void setNumberOfRows(Job job, long numRows) {
		job.getConfiguration().setLong(NUM_ROWS, numRows);
	}
	
	static void setNumberOfCols(Job job, String numCols) {
		job.getConfiguration().set("cols-num", numCols);
	}
	
	/**
	   * The Mapper class that given a row number, will generate the appropriate 
	   * output line.
	   */
	public static class SortGenMapper1 extends Mapper<LongWritable, NullWritable, Text, Text> {
	
	    private Text key = new Text();
	    private Text value = new Text();
	    private int colsNum;
	    private ZipfDistribution dist = null;
	    public int numOfElements = 0;
		public double exponent = 0;
		private Random random = new Random();
	    
	    private ArrayList<Element> generateRow() {
	    	ArrayList<Element> result = new ArrayList<Element>();
	    	
	    	for (int i=0; i<colsNum; i++) {
	    		if (i % 2 == 0) {
	    			result.add(new Element(random.nextInt(100000)));
	    		}
	    		else {
	    			result.add(new Element(dist.sample()));
	    		}
	    	}

	    	return result;
	    }
	    
	    private String getValueString(ArrayList<Element> attrs) {
	    	String result = "";
	    	String seperatedAttrs = StringUtils.join(" ", attrs);
	    	result += seperatedAttrs + "\r\n";
	    	return result;
	    }
	    
	    @Override
	    public void setup(Context context) {
	    	String colsAsStr = context.getConfiguration().get("cols-num");
	    	this.colsNum = Integer.parseInt(colsAsStr);
	    	String elementsAsStr = context.getConfiguration().get("elements");
	    	this.numOfElements = Integer.parseInt(elementsAsStr);
	    	String exponentAsStr = context.getConfiguration().get("exponenet");
	    	this.exponent = Double.parseDouble(exponentAsStr);
	    	this.dist = new ZipfDistribution(this.numOfElements, this.exponent);
	    }
	    
	    @Override
	    public void map(LongWritable row, NullWritable ignored,
	        Context context) throws IOException, InterruptedException {
	      	String keyStr = row.toString() + " ";
	    	key.set(keyStr.getBytes(), 0, keyStr.length());
	      	ArrayList<Element> attrs = generateRow();
	      	String valueStr = getValueString(attrs);
	      	value.set(valueStr.getBytes() , 0, valueStr.length());
	      	context.write(key, value);
	    }
	}
	
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance(getConf());
		if (args.length != 6) {
			usage();
			return 2;
		}
	    setNumberOfRows(job, parseHumanLong(args[0]));
	    setNumberOfCols(job,args[1]);
	    Path outputDir = new Path(args[2]);
	    if (outputDir.getFileSystem(getConf()).exists(outputDir)) {
	      throw new IOException("Output directory " + outputDir + 
	                            " already exists.");
	    }
	    int reducersNum = Integer.parseInt(args[3]);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setJobName("TeraDataBaseGenerator");
	    job.setJarByClass(TeraGen.class);
	    job.setMapperClass(SortGenMapper1.class);
	    job.setNumReduceTasks(reducersNum);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(RangeInputFormat.class);
	    job.setOutputFormatClass(TeraOutputFormat.class);
	    job.getConfiguration().set("elements", args[4]);
	    job.getConfiguration().set("exponenet",args[5]);
	    return job.waitForCompletion(true) ? 0 : 1;
	}

	  public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new ZipfGenerator(), args);
	    System.exit(res);
	  }
	
	
}
