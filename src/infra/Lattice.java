package infra;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Sets;

public class Lattice implements Serializable {

	private static final long serialVersionUID = 7057761844107022976L;
	private int numOfLevels;
	private HashMap<Integer,HashMap<CubeRegion,Table>> allCuboids;
	private ArrayList<CubeRegion> roots;
	private HashSet<Integer> dims = null;
	
	public HashSet<Integer> getDims() {
		return dims;
	}

	public Lattice(HashSet<Integer> dims) {
		this.dims = dims;
		numOfLevels = dims.size();
		buildLattice(dims);
	}
		
	public HashMap<Integer, HashMap<CubeRegion, Table>> getAllCuboids() {
		return allCuboids;
	}
	
	public ArrayList<CubeRegion> getRoots() {
		return roots;
	}
	
	private void buildLattice(Set<Integer> dims) {
		 roots = new ArrayList<CubeRegion>();
		 allCuboids = new HashMap<Integer,HashMap<CubeRegion,Table>>();
		 Set<Set<Integer>> dimsPowerSet = Sets.powerSet(dims);
		 ArrayList<Set<Integer>> powerSet = new ArrayList<Set<Integer>>(dimsPowerSet);
		 SetsComparator comp = new SetsComparator();
		 Collections.sort(powerSet,comp);
		 for (Set<Integer> dimsSubSet : powerSet) {
			 if (dimsSubSet.size() == 0) {
				 continue;
			 }
			 CubeRegion newRegion = new CubeRegion(dimsSubSet);
			 if (newRegion.getNumOfAttributes() == 1) {
				 roots.add(newRegion);
				 if (allCuboids.get(1) == null) {
					 allCuboids.put(1, new HashMap<CubeRegion,Table>());
				 }
				 allCuboids.get(1).put(newRegion,null);
			 }
			 else {
				 int level = newRegion.getNumOfAttributes();
				 if (allCuboids.get(level) == null) {
					 allCuboids.put(level, new HashMap<CubeRegion,Table>());
				 }
				 allCuboids.get(level).put(newRegion,null);
				 for (CubeRegion region : allCuboids.get(level-1).keySet()) {
					 if (region.isChild(newRegion)) {
						 region.addChild(newRegion);
						 newRegion.addAncestor(region);
					 }
				 }
			 }
		 }
	}
	
	public int getNumOfLevels() {
		return numOfLevels;
	}
	
	public int getBucket(CubeGroup r) {
		return 0;
	}
	
	public void addGroup(HashSet<Integer> cuboid, CubeGroup r) {
		HashMap<CubeRegion,Table> level = allCuboids.get(cuboid.size());
		CubeRegion region = new CubeRegion(cuboid);
		Table t = level.get(region);
		if (t != null) {
			t.addRow(r);
		}
		else {
			level.put(region, new Table());
			level.get(region).addRow(r);
		}
	}
	
	public void printSchema() {
		for (int i = getNumOfLevels(); i>=1; i--) {
			HashMap<CubeRegion,Table> levelRegions = allCuboids.get(i);
			System.out.println("level " + i);
			System.out.println("==========");
			for (CubeRegion r : levelRegions.keySet()) {
				System.out.println(r.getSchema());
			}
		}
	}
	
	public CubeRegion getRegion(Set<Integer> dimensions) {
		CubeRegion region = new CubeRegion(dimensions);
		Iterator<CubeRegion> it = allCuboids.get(dimensions.size()).keySet().iterator();
		while (it.hasNext()) {
			CubeRegion cr = it.next();
			if (cr.equals(region)) {
				return cr;
			}
		}
		return null;
	}
	
	public Table getTable(CubeRegion region) {
		int size = region.getRegion().size();
		return allCuboids.get(size).get(region);
	}
}
