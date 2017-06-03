package infra;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class UniqueRowInputFormat extends FileInputFormat<CubeGroup,NullWritable> {

	private static MRJobConfig lastContext = null;
	private static List<InputSplit> lastResult = null;
	
	static class RowRecordReader extends RecordReader<CubeGroup,NullWritable> {
	    private CubeGroup key;
	    private NullWritable value;
	    private FSDataInputStream in = null;
	    private LineNumberReader lnr = null;
	    private long totalLength = 0;
	    private long currentLength = 1;

	    public RowRecordReader() throws IOException {
	    	
	    }

	    @Override
	    public void initialize(InputSplit split, TaskAttemptContext context) 
	        throws IOException, InterruptedException {
	    	
	      Path p = ((FileSplit)split).getPath();
	      FileSystem fs = p.getFileSystem(context.getConfiguration());
	      in = fs.open(p);
	      lnr = new LineNumberReader(new InputStreamReader(in));
	      totalLength = split.getLength();
	    }

	    public void close() throws IOException {
	      in.close();
	    }

	    public CubeGroup getCurrentKey() {
	      return key;
	    }

	    public NullWritable getCurrentValue() {
	      return value;
	    }

	    public boolean nextKeyValue() throws IOException {
	    	String nextLine = lnr.readLine(); 
	    	if (nextLine == null) {
	    		return false;
	    	}
	    	nextLine = nextLine.trim();
	    	key = retrieveKey(nextLine);
	      	value = NullWritable.get();
	      	currentLength += nextLine.getBytes().length;
	      	return true;
	    }

		private CubeGroup retrieveKey(String line) {
			String[] dataAsStr = line.split(" ");
			TreeMap<Integer,Element> values = new TreeMap<Integer,Element>();
			int length = dataAsStr.length;
			int id = Integer.parseInt(dataAsStr[0]);
			int i=1;
			for (; i < length-1; i++) {
				values.put(i-1, new Element(Integer.parseInt(dataAsStr[i])));
			}
			Element measure = new Element(Integer.parseInt(dataAsStr[i]));
			CubeGroup result = new CubeGroup(id,values,measure);
			return result;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return currentLength / totalLength;
		}
	}
	
	
	@Override
	public RecordReader<CubeGroup,NullWritable> 
		createRecordReader(InputSplit split, TaskAttemptContext context) 
		throws IOException {
		return new RowRecordReader();
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		if (job == lastContext) {
			return lastResult;
	    }
	    long t1, t2;
	    t1 = System.currentTimeMillis();
	    lastContext = job;
	    
	    lastResult = super.getSplits(job);
	    t2 = System.currentTimeMillis();
	    System.out.println("Spent " + (t2 - t1) + "ms computing base-splits.");
	    return lastResult;
	}
}
