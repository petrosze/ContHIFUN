package evaluator.mapreduce.conceptualschema;

import org.apache.spark.api.java.function.Function;

import queryparts.Condition; 

public class Filtering implements Function<Tuple, Boolean> {

	private Condition condition;
	
	public Filtering(Condition condition) {
		this.condition = condition;
	}

	@Override
	public Boolean call(Tuple tuple) throws Exception {
		return condition.evalCondition(tuple.getAttributes().get(condition.getAttribute()));
	}

}
