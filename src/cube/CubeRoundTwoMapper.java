package cube;

import infra.CountAggregator;
import infra.CubeRegion;
import infra.Element;
import infra.Lattice;
import infra.LatticeIterator;
import infra.CubeGroup;
import infra.Operator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CubeRoundTwoMapper extends Mapper<LongWritable, Text, BytesWritable, BytesWritable>{
	
	private Lattice lattice;
	private CountAggregator  aggregator = new CountAggregator();
	
	private CubeGroup retrieveKey(String line) {
		line = line.trim();
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
		latticeDeserializer.close();
		latticeFileIn.close();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		aggregator.update(new Element(1));
		LinkedList<CubeRegion> regionsToHandle = new LinkedList<CubeRegion>();
		Iterator<CubeRegion> it = lattice.getRoots().iterator();
		CubeGroup retrieved = retrieveKey(value.toString());
		CubeGroup group = new CubeGroup(retrieved);
		while (it.hasNext()) {
			CubeRegion r = it.next();
			regionsToHandle.add(r);
		}
		
		while (!regionsToHandle.isEmpty()) {
			CubeRegion region = regionsToHandle.removeFirst();
			
			if (region.isAlreadyVisited()) {
				continue;
			}
			
			if (region.getAncestors() != null) {
				if (region.getNonSkewedParent(group) != null) {
					region.setAlreadyVisited(true);
					continue;
				}
//				for (CubeRegion ancestor : region.getAncestors()) {
//					if (ancestor.alreadyHandled(key,region)) {
//						region.setAlreadyVisited(true);
//						continue;
//					}
//				}
			}
			CubeGroup subGroup = group.getSubGroup(region.getRegion());
			if (region.containsSkew(subGroup)) {
				subGroup.setSkewed(true);
				region.addToSkew(subGroup, new CountAggregator());
				for (CubeRegion child : region.getChildren()) {
					regionsToHandle.add(child);
				}
			}
			else {
				CubeGroup k = new CubeGroup(subGroup);
				byte[] keyAsByteArray = CubeGroup.encodeKey(k);
				byte[] valueAsByteArray = CubeGroup.encodeValue(k);
				BytesWritable keyAsByteWritable = new BytesWritable(keyAsByteArray);
				BytesWritable valueAsIntWritable = new BytesWritable(valueAsByteArray);
				context.write(keyAsByteWritable,valueAsIntWritable);
			}
			region.setAlreadyVisited(true);
		}
		LatticeIterator latticeIt = new LatticeIterator(lattice);
		while (latticeIt.hasNext()) {
			CubeRegion region = latticeIt.next();
			region.setAlreadyVisited(false);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		LatticeIterator latticeIt = new LatticeIterator(lattice);
		while (latticeIt.hasNext()) {
			CubeRegion region = latticeIt.next();
			TreeMap<CubeGroup,Operator> partiallyAggregatedSkews = region.getSkews();
			for (CubeGroup skew : partiallyAggregatedSkews.keySet()) {
				if (partiallyAggregatedSkews.get(skew) == null) {
					continue;
				}
				CubeGroup group = new CubeGroup(skew);
				Element result = partiallyAggregatedSkews.get(skew).getFinalResult();
				group.setMeasure(result);
				byte[] keyAsByteArray = CubeGroup.encodeKey(group);
				byte[] valueAsByteArray = CubeGroup.encodeValue(group);
				BytesWritable keyAsByteWritable = new BytesWritable(keyAsByteArray);
				BytesWritable valueAsByteWritable = new BytesWritable(valueAsByteArray);
				context.write(keyAsByteWritable, valueAsByteWritable);
			}
		}
		CubeGroup noGrouping = new CubeGroup();
		noGrouping.setGroup(new HashSet<Integer>());
		Element measure = aggregator.getFinalResult();
		noGrouping.setMeasure(measure);
		byte[] keyAsByteArray = CubeGroup.encodeKey(noGrouping);
		byte[] valueAsByteArray = CubeGroup.encodeValue(noGrouping);
		BytesWritable keyAsByteWritable = new BytesWritable(keyAsByteArray);
		BytesWritable valueAsByteWritable = new BytesWritable(valueAsByteArray);
		context.write(keyAsByteWritable, valueAsByteWritable);
	}
	
}
