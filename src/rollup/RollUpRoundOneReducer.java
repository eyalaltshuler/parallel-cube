package rollup;

import infra.Element;
import infra.CubeGroup;
import infra.SumAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RollUpRoundOneReducer extends Reducer<CubeGroup, NullWritable, CubeGroup, NullWritable> {
	
	private MultipleOutputs<CubeGroup, NullWritable> outputs = null;
	private CubeGroup minValueRow = null;
	private CubeGroup maxValueRow = null;
	private int columnsNumber = 0;
	private ArrayList<SumAggregator> aggs = null;
	private ArrayList<SumAggregator> minRowAggs = null;
	private CubeGroup currentRow = null;
	private TreeSet<Integer> cols = new TreeSet<Integer>();
	private int minIndex = 0;
	private boolean minValueStillComputed = false;
	
	@Override
	public void setup(Context context) {
		outputs = new MultipleOutputs<CubeGroup, NullWritable>(context);
		Configuration conf = context.getConfiguration();
		String columnsAsStr = conf.get("table.colsnum");
		columnsNumber = Integer.parseInt(columnsAsStr) - 1; // -1 for not counting measure attr
		aggs = new ArrayList<SumAggregator>();
		for (int i=0; i<columnsNumber; i++) {
			aggs.add(new SumAggregator());
		}
		minRowAggs = new ArrayList<SumAggregator>();
		for (int i=0; i<columnsNumber; i++) {
			minRowAggs.add(new SumAggregator());
		}
		for (int k = 0; k < aggs.size(); k++){
			cols.add(k);
		}
	}
	
	@Override
	public void reduce(CubeGroup key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException {
		if (null == minValueRow) {
			minValueRow = new CubeGroup(key);
			maxValueRow = new CubeGroup(key);
			currentRow = new CubeGroup(key);
		}
		
		if (key.compareTo(minValueRow) < 0) {
			minValueRow = new CubeGroup(key);
		}
		
		minIndex = key.firstDifferentAttr(minValueRow);
		if (minIndex != 0) {
			minValueStillComputed = true;
			if (minIndex == -1) {
				minIndex = aggs.size();
			} 
			for (int j = minIndex - 1; j >= 0; j--) {
				Element measure = key.getElement(columnsNumber-1);
				minRowAggs.get(j).update(measure);
			}
		}
		else {
			minValueStillComputed = false;
		}
		
		int index = key.firstDifferentAttr(currentRow);
		if (index != -1) {
			TreeSet<Integer> tmpColsSet = new TreeSet<Integer>(cols);
			for (int j = aggs.size() - 1; j >= index ; j--) {
				Element measure = new Element(aggs.get(j).getFinalResult().getValue());
				CubeGroup r = currentRow.getSubGroup(tmpColsSet);
				r.setMeasure(measure);
				if ( (j != minIndex) || (currentRow.getElement(j).getValue() != (minValueRow.getElement(j).getValue()))) {
					outputs.write(r, NullWritable.get(),"finalround1");
				}
				aggs.get(j).clear();
				aggs.get(j).update(key.getElement(columnsNumber-1));
				tmpColsSet.remove(j);
			}
			if (!minValueStillComputed) {
				Element measure = key.getElement(columnsNumber-1);
				for (int j = index - 1; j >= 0 ; j--) {
					aggs.get(j).update(measure);
				}
			} else {
				Element measure = key.getElement(columnsNumber-1);
				for (int j = index - 1; j >= minIndex ; j--) {
					aggs.get(j).update(measure);
				}
			}
		}
		else if (!minValueStillComputed) {
			Element measure = key.getElement(columnsNumber-1);
			for (int i=0; i<aggs.size(); i++) {
				aggs.get(i).update(measure);
			}
		}
		currentRow = new CubeGroup(key);
		
		if (key.compareTo(maxValueRow) > 0) {
			maxValueRow = key;
		}
		
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		TreeSet<Integer> tmpColsSet = new TreeSet<Integer>(cols);
		for (int j=minRowAggs.size()-1; j>=0; j--) {
			CubeGroup r = minValueRow.getSubGroup(tmpColsSet);
			r.setMeasure(minRowAggs.get(j).getFinalResult());
			outputs.write("partial",r, NullWritable.get(),"partial/before");
			tmpColsSet.remove(j);
		}
		
		tmpColsSet = new TreeSet<Integer>(cols);
		int from = aggs.size()-1;
		int to = (minValueStillComputed) ? minIndex : 0 ;
		for (int j = from; j >= to ; j--) {
			CubeGroup r = maxValueRow.getSubGroup(tmpColsSet);
			r.setMeasure(aggs.get(j).getFinalResult());
			outputs.write("partial",r, NullWritable.get(),"partial/before");
			tmpColsSet.remove(j);
		}
				
		outputs.close();
	}
}
