package infra;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

//import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HDFSCube extends Cube {
	
	@SuppressWarnings("rawtypes")
	Context context = null;
	
	@SuppressWarnings("rawtypes")
	public HDFSCube(Context context) {
		this.context = context;
	}
	
	@Override
	public Table getCuboid(Set<Integer> dimensions) {
		return null;
	}
	
	@Override
	public boolean isAlreadyCalculated(Set<Integer> dimensions) {
		return false;
	}
	
	@Override
	public void insertCuboid(HashSet<Integer> dimensions, Table cuboid) {
		return;
	}
	 
	@SuppressWarnings("unchecked")
	@Override
	public void insertRowToCuboid(HashSet<Integer> dimensions, CubeGroup r) {
		try {
			context.write(r, NullWritable.get());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void setCuboidDimensionsNames(Set<Integer> dimensions, HashMap<Integer,String> names) {
		
	}
	
	@Override
	public Collection<Table> getCurrentCuboids() {
		return null;
	}
	
	@Override
	public HashMap<HashSet<Integer>, Table> getCuboids() {
		return null;
	}

	@Override
	public void setCuboids(HashMap<HashSet<Integer>, Table> cuboids) {
		return;
	}

	@Override
	public String toString() {
		return "";
	}
}
