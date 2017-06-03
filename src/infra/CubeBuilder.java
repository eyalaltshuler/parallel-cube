package infra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class CubeBuilder {
	private Table table = null;
	private Cube cube = null;
	private RowsComparator rowsComparator = null;
	private Operator aggregator;
	private int id = 0;
	private Filter filter = null;
	
	public CubeBuilder(Table t, Cube cube) {
		this.table = t;
		this.cube = cube;
		rowsComparator = new RowsComparator();
	}
	
	private void recursiveCalculate(HashMap<Integer,Element> alreadyAggregated, 
									HashSet<Integer> notAggregatedYet, 
									Integer dimension, 
									int start, 
									int end) {
		if (start >= end) {
			return;
		}
		
		// init
		notAggregatedYet.remove(dimension);
		
		// first, sort data by dimension
		ArrayList<Integer> dimensionsOrder = new ArrayList<Integer>();
		dimensionsOrder.add(dimension);
		rowsComparator.setDimensionToCompare(dimensionsOrder);
		Collections.sort(table.getRows().subList(start, end) , rowsComparator);
		
		int i = start;
		int from = start;
		
		Element currentAggregated = new Element(table.getRow(start).getValues().get(dimension).getValue());
		Element currentElement = null;
		CubeGroup generatedRow = null;
		//ArrayList<Element> aggregated = null;
		aggregator.clear();
		
		// recursively aggregate bottom up
		do {
			currentElement = table.getRow(i).getElement(dimension); 
			CubeGroup currentRow = table.getRow(i);
			// check whether the next row has the same value in the required dimension
			if ((currentElement.getValue()) != currentAggregated.getValue()) {
				Element result = aggregator.getFinalResult();
				
				// write result to the cube
				alreadyAggregated.put(dimension, currentAggregated);
				TreeMap<Integer,Element> dataForGeneratedRow = generateData(alreadyAggregated,result);
				generatedRow = new CubeGroup(id++, dataForGeneratedRow, result);
				generatedRow.setGroup(alreadyAggregated.keySet());
				HashSet<Integer> key = new HashSet<Integer>(alreadyAggregated.keySet());
				if (this.filter.isValid(generatedRow)) {
					this.cube.insertRowToCuboid(key, generatedRow);
				}
				
				HashSet<Integer> smallerSet = new HashSet<Integer>(notAggregatedYet);
				// call recursively to the rest set of attributes
				for (Integer j : notAggregatedYet) {
					smallerSet.remove(j);
					recursiveCalculate(new HashMap<Integer,Element>(alreadyAggregated), new HashSet<Integer>(smallerSet), j, from, i);
				}
				
				from = i;
				aggregator.clear();
				aggregator.update(currentRow.getMeasure());
				currentAggregated = currentElement;
			}
			else {
				aggregator.update(currentRow.getMeasure());
			}
			i++;
		} while (i < end);
		
		Element result = aggregator.getFinalResult();
		
		// write result to the cube
		alreadyAggregated.put(dimension, currentAggregated);
//		aggregated = new ArrayList<Element>(alreadyAggregated.values());
//		aggregated.add(result);
		TreeMap<Integer,Element> dataForGeneratedRow = generateData(alreadyAggregated,result);
		generatedRow = new CubeGroup(id++, dataForGeneratedRow,result);
		generatedRow.setGroup(alreadyAggregated.keySet());
		HashSet<Integer> key = new HashSet<Integer>(alreadyAggregated.keySet());
		if (this.filter.isValid(generatedRow)) {
			this.cube.insertRowToCuboid(key, generatedRow);
		}
		
		HashSet<Integer> smallerSet = new HashSet<Integer>(notAggregatedYet);
		// call recursively to the rest set of attributes
		for (Integer j : notAggregatedYet) {
			smallerSet.remove(j);
			recursiveCalculate(new HashMap<Integer,Element>(alreadyAggregated), new HashSet<Integer>(smallerSet), j, from, i);
		}
		
		from = i+1;
		aggregator.clear();
	}
	
	private TreeMap<Integer,Element> generateData(
			HashMap<Integer, Element> alreadyAggregated,
			Element result) {
		TreeMap<Integer,Element> data = new TreeMap<Integer,Element>();
		
		for (Map.Entry<Integer, Element> entry : alreadyAggregated.entrySet()) {
			int dim = entry.getKey();
			int value = entry.getValue().getValue();
			Element e = new Element(value);
			data.put(dim,e);
		}
		return data;
	}

	public Cube buildCube(HashSet<Integer> dimensions, Operator op, Filter filter, HashMap<Integer,Element> alreadyAggregated) {
		this.aggregator = op;
		HashSet<Integer> reducedDimensions = new HashSet<Integer>(dimensions);
		TreeSet<Integer> sortedDims = new TreeSet<Integer>(dimensions);
		this.filter = filter;
		aggregator.clear();
		
		HashMap<Integer,Element> mappedAttrs = null;
		
		for (Integer dim : sortedDims) {
			if (alreadyAggregated != null) {
				mappedAttrs = new HashMap<Integer,Element>(alreadyAggregated);
			}
			else {
				mappedAttrs = new HashMap<Integer,Element>();
			}
			recursiveCalculate(mappedAttrs, reducedDimensions, dim, 0, table.getRows().size());
			aggregator.clear();
			reducedDimensions.remove(dim);
		}
		
		return this.cube;
	}
	
	public Cube buildCubeForGroup(HashSet<Integer> dimensions, Operator op, Filter filter, int from, int to, HashMap<Integer,Element> alreadyAggregated) {
		this.aggregator = op;
		HashSet<Integer> reducedDimensions = new HashSet<Integer>(dimensions);
		TreeSet<Integer> sortedDims = new TreeSet<Integer>(dimensions);
		this.filter = filter;
		aggregator.clear();
		
		HashMap<Integer,Element> mappedAttrs = new HashMap<Integer,Element>();
		
		for (Integer dim : sortedDims) {
			if (alreadyAggregated != null) {
				mappedAttrs = new HashMap<Integer,Element>(alreadyAggregated);
			}
			recursiveCalculate(mappedAttrs, reducedDimensions, dim, from, to);
			aggregator.clear();
			reducedDimensions.remove(dim);
		}
		
		return this.cube;
	}
}
