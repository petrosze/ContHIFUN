package evaluator.mapreduce.measures;

public class AvgMValue extends MValue{

	private Object avg_sum;
	private Integer avg_count;
	
	public AvgMValue() {
		
	}

	public AvgMValue(Object sum, Integer count) {
		this.avg_sum = sum;
		this.avg_count= count;
	}
	
	public Object getSum() {
		return this.avg_sum;
	}
	
	public Integer getCount() {
		return this.avg_count;
	}
	
	@Override
	public void setMValue(Object val) {
		this.avg_count = 1;
		this.avg_sum = val;
		
	}

	@Override
	public String toString() {
		return "AvgMValue [avg_sum=" + avg_sum + ", avg_count=" + avg_count + "]";
	}

	@Override
	public Object getMValue() {
		return this.avg_sum;
	}
}
