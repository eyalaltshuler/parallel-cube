package infra;

public class CountAggregator implements Operator{

	private static final long serialVersionUID = -5120702673969800236L;
	private int count = 0;
	
	@Override
	public void update(Element element) {
		count++;		
	}

	@Override
	public Element getFinalResult() {
		return new Element(count);
	}

	@Override
	public void clear() {
		count = 0;		
	}

}
