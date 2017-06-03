package infra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Cube {
	private HashMap<HashSet<Integer>,Table> cuboids = new HashMap<HashSet<Integer>,Table>();
	
	/**
	 * Get a specific cuboid
	 * 
	 * @param dimensions
	 * @return The specific cuboid identified with dimensions
	 */
	public Table getCuboid(Set<Integer> dimensions) {
		return cuboids.get(dimensions);
	}
	
	/**
	 * Check whether a specific cuboid has already been calculated
	 * 
	 * @param dimensions
	 * @return true if the cuboid exists, false otherwise
	 */
	public boolean isAlreadyCalculated(Set<Integer> dimensions) {
		return cuboids.containsKey(dimensions);
	}
	
	/**
	 * insert a cuboid to the cube
	 * 
	 * @param dimensions
	 * @param cuboid
	 */
	public void insertCuboid(HashSet<Integer> dimensions, Table cuboid) {
		cuboids.put(dimensions, cuboid);
	}
	
	/**
	 * Given a row and a specific cuboid, add the row to the cuboid
	 * 
	 * @param dimensions
	 * @param r
	 */
	public void insertRowToCuboid(HashSet<Integer> dimensions, CubeGroup r) {
		if (!this.cuboids.containsKey(dimensions)) {
			Table cuboid = new Table();
			this.cuboids.put(dimensions, cuboid);
		}
		this.cuboids.get(dimensions).addRow(r);
	}

	/**
	 * 
	 * @param dimensions
	 * @param names
	 */
	public void setCuboidDimensionsNames(Set<Integer> dimensions, HashMap<Integer,String> names) {
		
	}
	
	public Collection<Table> getCurrentCuboids() {
		return cuboids.values();
	}
	
	public HashMap<HashSet<Integer>, Table> getCuboids() {
		return cuboids;
	}

	public void setCuboids(HashMap<HashSet<Integer>, Table> cuboids) {
		this.cuboids = cuboids;
	}

	@Override
	public String toString() {
		String endline = "\r\n";
		String result = "Cube is : " + endline;
		result += "=============================" + endline;
		Set<HashSet<Integer>> cuboids = this.cuboids.keySet();
		for (HashSet<Integer> c : cuboids) {
			ArrayList<Integer> tmp = new ArrayList<Integer>(c);
			result += "printing cube group " + tmp.toString() + endline;
			Table t = this.cuboids.get(c);
			for (CubeGroup r : t.getRows()) {
				result += r.toString() + endline;
			}
		}
		return result;
	}
}
