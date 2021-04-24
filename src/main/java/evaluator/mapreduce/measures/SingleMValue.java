package evaluator.mapreduce.measures;

public class SingleMValue extends MValue{
	
	private Object mvalue;
	
	public SingleMValue() {
	
	}

	public SingleMValue(Object val) {
		this.mvalue = val;
	}
	
	@Override
	public void setMValue(Object val) {
		this.mvalue = val;
	}

	@Override
	public String toString() {
		return "SingleMValue [mvalue=" + mvalue + "]";
	}

	@Override
	public Object getMValue() {
		return this.mvalue;
	}
}
