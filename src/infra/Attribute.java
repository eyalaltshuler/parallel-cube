package infra;

import java.io.Serializable;

public abstract class Attribute implements Serializable {
	
	private static final long serialVersionUID = -1082932675665955606L;
	private String name;
	private int attrId;
	private Comparable<?> value;
	
	public Comparable<?> getValue() {
		return value;
	}

	public Attribute() {
		
	}
	
	public Attribute(String name, int attrId) {
		super();
		this.name = name;
		this.attrId = attrId;
	}
	
	public String getName() {
		return name;
	}
	
	public int getAttrId() {
		return attrId;
	}
}
