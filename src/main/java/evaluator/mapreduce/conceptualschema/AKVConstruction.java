package evaluator.mapreduce.conceptualschema;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFunction;

import context.Context;
import evaluator.mapreduce.measures.MValue;
import query.Query;
import scala.Tuple2;

public class AKVConstruction implements PairFunction<Tuple2<Key, ArrayList<MValue>>, Key, ArrayList<MValue>>{

	private Query query;
	private Context context; 
	
	public AKVConstruction(Query query, Context cnxt) {
		this.query = query;
		this.context = cnxt;
	}

	@Override
	public Tuple2<Key, ArrayList<MValue>> call(Tuple2<Key, ArrayList<MValue>> t) throws Exception {
		
		Key key = t._1;
		
		String attrName = query.getQueryTriple()._1().getGroupingAttribute();
		String attrValue = key.getValue();
		
		Key newKey = new Key();
		newKey.addAttributeValue(attrName, context.getFunctions().get(attrName).apply(attrValue).toString());
		
		return new Tuple2<>(newKey, t._2);
	}
}
