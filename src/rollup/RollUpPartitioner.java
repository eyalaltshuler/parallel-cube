package rollup;

import infra.CubeGroup;
import infra.RowsComparator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RollUpPartitioner extends Partitioner<CubeGroup,NullWritable> implements Configurable {

	private TreeSet<CubeGroup> splitPoints;

    private Configuration conf;
    
    private static TreeSet<CubeGroup> readPartitions(FileSystem fs, Path p, Configuration conf) throws IOException, ClassNotFoundException {
    	int reduces = conf.getInt("reduces-num", 1);
    	RowsComparator attrsComparator = new RowsComparator();
	    //attrsComparator.setDimensionToCompare(attributesOrder);
	    TreeSet<CubeGroup> samples = new TreeSet<CubeGroup>(attrsComparator);
	    TreeSet<CubeGroup> result = new TreeSet<CubeGroup>(attrsComparator);
	    FSDataInputStream reader = fs.open(p);
	    BufferedReader br = new BufferedReader(new InputStreamReader(reader));
	    String line = br.readLine();
	    while (line != null) {
	    	CubeGroup r = new CubeGroup();
	    	r.parseFromString(r, line);
	    	line = br.readLine();
	    	samples.add(r);
	    }

	    System.out.println("Making " + reduces + " partitions from " + samples.size() + " samples");
	    float stepSize = samples.size() / (float) reduces;
	    ArrayList<CubeGroup> tmp = new ArrayList<CubeGroup>(samples);
	    for (int i=0; i<reduces; i++) {
	    	result.add(tmp.get(Math.round(i * stepSize)));
	    }
	    br.close();
	    reader.close();
	    return result;
    }
    
    public void setConf(Configuration conf) {
    	try {
    		FileSystem fs = FileSystem.getLocal(conf);
	        this.conf = conf;
	        Path partFile = new Path("part-r-00000");
	        splitPoints = readPartitions(fs, partFile, conf);
	    } catch (IOException ie) {
	        throw new IllegalArgumentException("can't read partitions file", ie);
	    } catch (ClassNotFoundException cnfe) {
	    	throw new IllegalArgumentException("can't read partitions file", cnfe);
	    }
    }

	public Configuration getConf() {
      return conf;
    }
	    
    public RollUpPartitioner() {
    }

	@Override
	public int getPartition(CubeGroup row, NullWritable value, int NumOfPartitions) {
		return splitPoints.headSet(row).size();
	}
}


