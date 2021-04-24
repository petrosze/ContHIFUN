package evaluator.mapreduce.measures;

import java.io.Serializable;

public abstract class MValue implements Serializable{
	
	public abstract void setMValue(Object val);
	public abstract Object getMValue();
	public abstract String toString();
}
