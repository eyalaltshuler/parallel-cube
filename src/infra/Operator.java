package infra;

import java.io.Serializable;

public interface Operator extends Serializable {
		
	public void update(Element element);
	
	public Element getFinalResult();
	
	public void clear();
}
