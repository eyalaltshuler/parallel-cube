package cube;

import infra.CountAggregator;
import infra.CountFilter;
import infra.Cube;
import infra.CubeBuilder;
import infra.CubeRegion;
import infra.Lattice;
import infra.LatticeIterator;
import infra.CubeGroup;
import infra.Operator;
import infra.RowsComparator;
import infra.Table;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CubeRoundOneReducer extends Reducer<BytesWritable,NullWritable,Text,NullWritable>{

	ArrayList<CubeGroup> sampledRows = new ArrayList<CubeGroup>();
	
	private TreeSet<Integer> dims = new TreeSet<Integer>();
	
	private int threshold;
	
	private int buckets;
	
	private void retrieveDims(String dimsAsStr) {
		String[] seperatedDims = dimsAsStr.split(",");
		Integer attr = null;
		for (int i=0; i<seperatedDims.length; i++) {
			attr = Integer.parseInt(seperatedDims[i]);
			dims.add(attr);
		}
	}
	
	@Override
	public void setup(Context context) {
		// understand from configuration what is the cube query
		String dimsAsStr = context.getConfiguration().get("dims");
		retrieveDims(dimsAsStr);
		threshold = Integer.parseInt(context.getConfiguration().get("threshold"));
		buckets = Integer.parseInt(context.getConfiguration().get("buckets"));
		CubeGroup.dimensions = dims;
	}
	
	@Override
	public void reduce(BytesWritable key, Iterable<NullWritable> value, Context context) {
		CubeGroup group = CubeGroup.decode(key.getBytes(), true, true);
		sampledRows.add(group); 
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {	
		Lattice lattice = new Lattice(new HashSet<Integer>(dims));
		
		// for every member of power set of dims , calculate bucketing elements
		// and add to annotated lattice
		RowsComparator comp = new RowsComparator();
		LatticeIterator latticeIt = new LatticeIterator(lattice);
		while (latticeIt.hasNext()) {
			CubeRegion r = latticeIt.next();
			comp.setDimensionToCompare(new ArrayList<Integer>(r.getRegion()));
			Collections.sort(sampledRows , comp);
			ArrayList<Integer> dimsToCompare = new ArrayList<Integer>();
			for (Integer d : r.getRegion()) {
				dimsToCompare.add(d);
			}
			TreeSet<CubeGroup> boundaries = RetrieveBoundaries(r.getRegion());
			r.setBuckets(boundaries);
		}
		
		// for the entire sample, calculate BUC with count as an aggregate function
		Cube cube = new Cube();
		Table t = new Table();
		for (CubeGroup r : sampledRows) {
			t.addRow(r);
		}
		CubeBuilder cubeBuilder = new CubeBuilder(t,cube);
		cube = cubeBuilder.buildCube(new HashSet<Integer>(dims), new CountAggregator(), new CountFilter(threshold), null);
			
		// add skews to the lattice
		Set<Map.Entry<HashSet<Integer>,Table>> entries = cube.getCuboids().entrySet();
		TreeMap<CubeGroup,Operator> skews = null;
		for (Map.Entry<HashSet<Integer>,Table> pair : entries ) {
			CubeRegion region = lattice.getRegion(pair.getKey());
			skews = new TreeMap<CubeGroup,Operator>();
			for (CubeGroup skew : pair.getValue().getRows()) {
				skew.setGroup(region.getRegion());
				skew.setSkewed(true);
				skews.put(new CubeGroup(skew), null);
			}
			region.setSkews(skews);
		}
		
		// write lattice to HDFS
		Path annotatedLatticePath = new Path("annotatedLattice");
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataOutputStream fileOut = fs.create(annotatedLatticePath);
		ObjectOutputStream latticeSerializer = new ObjectOutputStream(fileOut);
		
		latticeSerializer.writeObject(lattice);
		
		latticeSerializer.flush();
		latticeSerializer.close();
		fileOut.flush();
		Integer fileOutSize = new Integer(fileOut.size());
		Text sketchSize = new Text(fileOutSize.toString());
		context.write(sketchSize, NullWritable.get());
		fileOut.close();
	}

	private TreeSet<CubeGroup> RetrieveBoundaries(HashSet<Integer> dims) {
		RowsComparator comp = new RowsComparator();
		comp.setDimensionToCompare(new ArrayList<Integer>(dims));
		TreeSet<CubeGroup> result = new TreeSet<CubeGroup>(comp);
		if (buckets <= 1) {
			return result;
		}
		int numOfBoundaries = 0;
		double step = 0.0;
		if (sampledRows.size() <= buckets - 1) {
			numOfBoundaries = sampledRows.size();
			step = 1;
		}
		else {
			numOfBoundaries = buckets - 1;
			step = sampledRows.size() / (numOfBoundaries + 1); 
		}
		
		int i=0;
		CubeGroup tmp;
		for (i=1; i<=numOfBoundaries; i++) {
			int index = (int)(Math.ceil(i * step));
			if (index > sampledRows.size() - 1) {
				break;
			}
			tmp = new CubeGroup(sampledRows.get(index));
			ArrayList<Integer> dimsToCompare = new ArrayList<Integer>();
			for (Integer d : dims) {
				dimsToCompare.add(d);
			}
			result.add(tmp);
		}
		
		return result;
	}
}
