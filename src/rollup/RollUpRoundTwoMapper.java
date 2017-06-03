package rollup;

//import infra.Element;
import infra.CubeGroup;
import infra.SumAggregator;

import java.io.IOException;
//import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RollUpRoundTwoMapper extends Mapper<CubeGroup, NullWritable, CubeGroup, NullWritable> {
	
	private HashMap<CubeGroup,SumAggregator> partialAggregates = null;
	
	@Override
	public void setup(Context context) {
		partialAggregates = new HashMap<CubeGroup, SumAggregator>();
	}
	
	@Override
	public void map(CubeGroup key, NullWritable value, Context context) {
//		ArrayList<Element> dimensionAttrs = new ArrayList<Element>();
//		for (int i=0; i<key.getDimensionsNum()-1; i++) {
//			dimensionAttrs.add(new Element(key.getElement(i).getValue()));
//		}
//		CubeGroup r = new CubeGroup(key);
//		if (partialAggregates.get(r) == null) {
//			partialAggregates.put(r, new SumAggregator());
//		}
//		partialAggregates.get(r).update(key.getElement(key.getDimensionsNum()-1));
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<CubeGroup, SumAggregator> entry : partialAggregates.entrySet()) {
			CubeGroup r = entry.getKey();
			SumAggregator agg = entry.getValue();
			r.setMeasure(agg.getFinalResult());
			context.write(r, NullWritable.get());
		}
	}

}
