package evaluator.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import context.Context;


public class RowCreation implements FlatMapFunction<String, Row> {

	private Map<String, java.util.function.Function<Object, Object>> functions; 
	private ArrayList<String> usefulAttributes; 
 
	
	public RowCreation(Context context, Set<String> attr) {
		this.functions = context.getFunctions();
		this.usefulAttributes = new ArrayList<>(attr);
	}
	
	@Override
	public Iterator<Row> call(String srow) throws Exception {
		
		List<Object> data = new ArrayList<>();
		for(String attr : usefulAttributes){
			java.util.function.Function<Object, Object> function = functions.get(attr);
			Object value = function.apply(srow);
			data.add(value); 
		}
		ArrayList<Row> list = new ArrayList<>();
		list.add(RowFactory.create(data.toArray()));	
		return list.iterator();
	}

}
