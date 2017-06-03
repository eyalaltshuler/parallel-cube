package cube;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Reducer;

import infra.CountAggregator;
import infra.Cube;
import infra.CubeBuilder;
import infra.CubeRegion;
import infra.Element;
import infra.HDFSCube;
import infra.Lattice;
import infra.LatticeIterator;
import infra.NoFilter;
import infra.CubeGroup;
import infra.RowsComparator;
import infra.SumAggregator;
import infra.Table;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.Iterable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

public class CubeRoundTwoReducer extends Reducer<BytesWritable, BytesWritable, CubeGroup, NullWritable>{

	private Lattice lattice;
	
	private SumAggregator aggregator = new SumAggregator();
	
	@Override
	public void setup(Context context) throws IOException {
		aggregator.clear();
		Path annotatedLatticePath = new Path("annotatedLattice");
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream latticeFileIn = fs.open(annotatedLatticePath);
		ObjectInputStream latticeDeserializer = new ObjectInputStream(latticeFileIn);
		try {
			lattice = (Lattice) latticeDeserializer.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		CubeGroup.dimensions = new TreeSet<Integer>(lattice.getDims());
		latticeDeserializer.close();
		latticeFileIn.close();
		LatticeIterator latticeIt = new LatticeIterator(lattice);
		CubeRegion region = null;
		while (latticeIt.hasNext()) {
			region = latticeIt.next();
			region.resetSkews();
		}
	}
	
	@Override
	public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
		byte[] keyData = key.getBytes();
		boolean groupSkewed = (keyData[0]==0) ? false : true;
		boolean zeroGroup = (keyData[1]==0) ? true : false;
		if (groupSkewed) {
			int result = 0;
			for (BytesWritable value : values) {
				result += WritableComparator.readInt(value.getBytes(), 0);
			}
			Element measure = new Element(result);
			CubeGroup group = CubeGroup.decode(key.getBytes(),false,false);
			group.setMeasure(measure);
			context.write(group, NullWritable.get());
		}
		else if (zeroGroup) {
			int result = 0;
			for (BytesWritable value : values) {
				result += WritableComparator.readInt(value.getBytes(), 0);
			}
			CubeGroup group = new CubeGroup();
			group.setGroup(new HashSet<Integer>());
			group.setMeasure(new Element(result));
			context.write(group, NullWritable.get());
		}
		else {
			CountAggregator aggregator = new CountAggregator();
			aggregator.clear();
			Table t = new Table();
			CubeGroup groupToAdd = null;
			for (BytesWritable value : values) {
				groupToAdd = CubeGroup.decodeKeyValue(key,value);
				t.addRow(groupToAdd);
				aggregator.update(new Element(1));
			}
			
			recursiveCube(groupToAdd, t, 0, t.getRows().size(), context);	
		}
	}

	private void recursiveCube(CubeGroup cubeGroup, Table t, int from, int to, Context context) throws IOException, InterruptedException {
		CountAggregator aggregator = new CountAggregator();
		aggregator.clear();
		for (int i=from; i<to; i++) {
			aggregator.update(t.getRow(i).getMeasure());
			t.getRow(i).setGroup(cubeGroup.getDims());
		}
		Element measure = aggregator.getFinalResult();
		CubeGroup groupToAdd = new CubeGroup(cubeGroup);
		groupToAdd.setMeasure(measure);
		context.write(groupToAdd, NullWritable.get());
		
		CubeGroup group = new CubeGroup(groupToAdd);
		Cube cube = new HDFSCube(context);
		
		
		// cube direct children
		TreeSet<Integer> dims = new TreeSet<Integer>(group.getDims());
		int maxDim = dims.last();
		HashSet<Integer> cubingDims = new HashSet<Integer>();
		for (int i=maxDim+1; i<lattice.getNumOfLevels(); i++) {
			cubingDims.add(i);
		}
		
		CubeBuilder cubeBuilder = null;
		if (cubingDims.size() > 0) {
			cubeBuilder = new CubeBuilder(t, cube);
			HashMap<Integer, Element> mappedAttrs = new HashMap<Integer, Element>();
			for (Integer d : group.getDims()) {
				mappedAttrs.put(d, group.getValues().get(d));
			}
			cubeBuilder.buildCubeForGroup(cubingDims, new CountAggregator(), new NoFilter(), from, to, mappedAttrs);
		}
		
		// cube indirect children
		CubeRegion cubeRegion = lattice.getRegion(group.getDims());
		RowsComparator comp = new RowsComparator();
		int index = from;
		int tmpIndex = index;
		CubeGroup tmpRow = null;
		CubeGroup currRow = null;
		
		for (CubeRegion child : cubeRegion.getChildren()) {
			if (cubeRegion.directChild(child)) {
				continue;
			}
			HashSet<Integer> childDims = child.getRegion();
			comp.setDimensionToCompare(new ArrayList<Integer>(childDims));
			Collections.sort(t.getRows().subList(from, to), comp);
			for (int i=from; i<to; i++) {
				t.getRow(i).setGroup(childDims);
			}
			tmpRow = t.getRow(from);
			index = from;
			tmpIndex = index;
			do {
				currRow = t.getRow(index);
				if (!currRow.equals(tmpRow)) {
					if (checkChild(tmpRow, lattice.getRegion(group.getDims()), child)) {
						CubeGroup childGroup = new CubeGroup(tmpRow);
						childGroup.setGroup(child.getRegion());
						recursiveCube(childGroup, t, tmpIndex, index, context);
					}						
					tmpRow = currRow;
					tmpIndex = index;
				}
				index++;
			} while (index < to);
			if (checkChild(tmpRow, lattice.getRegion(group.getDims()), child)) {
				CubeGroup childGroup = new CubeGroup(tmpRow);
				childGroup.setGroup(child.getRegion());
				recursiveCube(childGroup, t, tmpIndex, index, context);
			}
		}
	}

	private boolean checkChild(CubeGroup currentRow, CubeRegion region, CubeRegion child) {
		if (region.getChildren() != null) {
			if (region.equals(child.getNonSkewedParent(currentRow))) {
				return true;
			}
		}
		return false;
	}
}



