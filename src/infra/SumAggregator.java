package infra;

public class SumAggregator implements Operator {

	private static final long serialVersionUID = -1774358110771143156L;
	private int sum = 0;
	@Override
	public void update(Element element) {
		sum += element.getValue();
	}

	@Override
	public Element getFinalResult() {
		return new Element(sum);
	}

	@Override
	public void clear() {
		sum = 0;
	}

}
