package queryparts;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import defines.Operations;
import evaluator.mapreduce.measures.AvgMValue;
import evaluator.mapreduce.measures.MValue;
import evaluator.mapreduce.measures.SingleMValue; 

public class Operation implements Serializable{
	
	private List<Operations> operations;

	public Operation(Operations operation) {
		operations = new ArrayList<>();
		operations.add(operation);
	}

	public Operation() {
		operations = new ArrayList<>();
	}
	
	public void addOperation(Operation o) {
		this.operations.add(o.getOperation());
	}
	
	public Operations getOperation() {
		if(this.operations != null) return this.operations.get(0);
		else return null;
	}
	
	public List<Operations> getOperations() {
		return this.operations;
	}
	
	public MValue applyOperation(MValue val1, MValue val2, Operations op) {
		
		switch(op) {
			case MIN: 
				Object min = null;
				if (val1.getMValue() instanceof Integer) {
					min = Math.min(((Integer) val1.getMValue()).intValue(), ((Integer) val2.getMValue()).intValue());
				}
				return new SingleMValue(min);
				
			case MAX:
				Object max = null;
				if (val1.getMValue() instanceof Integer) {
					max = Math.max(((Integer) val1.getMValue()).intValue(), ((Integer) val2.getMValue()).intValue());
				}
				return new SingleMValue(max);
				
			case SUM: 
				Object sum = null;
				if (val1.getMValue() instanceof Integer) {
					sum = ((Integer) val1.getMValue()).intValue() + ((Integer) val2.getMValue()).intValue();
				}
				return new SingleMValue(sum);
	
			case COUNT:
				Object count = null;
				if (val1.getMValue() instanceof Integer) {
					count = ((Integer) val1.getMValue()).intValue() + ((Integer) val2.getMValue()).intValue();
				}
				return new SingleMValue(count);
				
			case AVG: 
				
				Object avg_sum = null;
				Integer avg_count = null;
				AvgMValue v1 = (AvgMValue) val1;
				AvgMValue v2 = (AvgMValue) val2;
				
				if (v1.getSum() instanceof Integer) {
					avg_sum = ((Integer) v1.getSum()).intValue() + ((Integer) v2.getSum()).intValue();	
				}
				avg_count = v1.getCount() + v2.getCount();
				
				return new AvgMValue(avg_sum, avg_count);
				
			case NON:	
				return null;
		}
		return null;
	}
	
	@Override
	public String toString() {
		return "Operation [operations=" + operations + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((operations == null) ? 0 : operations.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Operation other = (Operation) obj;
		if (operations == null) {
			if (other.operations != null)
				return false;
		} else if (!operations.equals(other.operations))
			return false;
		return true;
	}


}
