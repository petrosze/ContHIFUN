package evaluator.mapreduce.conceptualschema;
 
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.Function;

import context.Context; 

public class InputPreparation implements Function<String, Tuple> {
	
	private Map<String, java.util.function.Function<Object, Object>> functions; 
	private Set<String> usefulAttributes; 

	public InputPreparation(Context cxt, Set<String> attr) {
		this.functions = cxt.getFunctions();
		this.usefulAttributes = attr;
	}

	
	@Override
	public Tuple call(String line) throws Exception {
		
		Tuple tuple = new Tuple(); 
		for(String attr : usefulAttributes){
			java.util.function.Function<Object, Object> function = functions.get(attr);
			tuple.addAttribute(attr, function.apply(line));
		}
		return tuple;
	}
}
