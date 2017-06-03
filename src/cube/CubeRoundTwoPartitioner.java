package cube;

import infra.CubeRegion;
import infra.Lattice;
import infra.CubeGroup;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CubeRoundTwoPartitioner extends Partitioner<BytesWritable, BytesWritable> 
	implements Configurable {
	
	private Lattice lattice;
	private Configuration conf;
	
	public void setConf(Configuration conf) {
    	try {
    		this.conf = conf;
    		FileSystem fs = FileSystem.get(conf);
	        Path latticeFile = new Path("annotatedLattice");
	        FSDataInputStream fileIn = fs.open(latticeFile);
	        ObjectInputStream latticeDeserializer = new ObjectInputStream(fileIn);
	        lattice = (Lattice) latticeDeserializer.readObject();
	        latticeDeserializer.close();
	        fileIn.close();
	    } catch (IOException ie) {
	        throw new IllegalArgumentException("can't read annotated lattice file", ie);
	    } catch (ClassNotFoundException e) {
			e.printStackTrace();
		}	
    }
	
	@Override
	public int getPartition(BytesWritable key, BytesWritable value, int numOfPartitions) {
		CubeGroup.dimensions = new TreeSet<Integer>(lattice.getDims());
		CubeGroup group = CubeGroup.decodeKeyValue(key, value);
		if (group.isSkewed() || (group.getGroup() == 0)) {
			return 0;
		}
		Set<Integer> groupDimensions = group.getDims();
		HashSet<Integer> dimsAsHasSet = new HashSet<Integer>(groupDimensions);
		CubeRegion region = lattice.getRegion(dimsAsHasSet);
		int bucket = region.getBucket(group); 
		return 1 + bucket;
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}
}
