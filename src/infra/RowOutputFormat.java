package infra;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RowOutputFormat extends FileOutputFormat<CubeGroup, NullWritable>{

	  static class RowRecordWriter extends RecordWriter<CubeGroup,NullWritable> {
		  
		  private FSDataOutputStream out;
		  
		  public RowRecordWriter(FSDataOutputStream fileOut, TaskAttemptContext job) {
			this.out = fileOut;
		  }

		  @Override
		  public void close(TaskAttemptContext arg0) throws IOException,
			InterruptedException {
			this.out.flush();
			this.out.close();
		  }

		  @Override
		  public void write(CubeGroup key, NullWritable value) throws IOException,
		  	InterruptedException {
			out.write(key.toString().getBytes());
		  }
		
	  }
	
	  @Override
	  public RecordWriter<CubeGroup, NullWritable> getRecordWriter(TaskAttemptContext job) 
			throws IOException, InterruptedException {
		  Path file = getDefaultWorkFile(job, "");
		  FileSystem fs = file.getFileSystem(job.getConfiguration());
		  FSDataOutputStream fileOut = fs.create(file);
		  return new RowRecordWriter(fileOut, job);
	  }
}
