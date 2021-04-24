package evaluator.mapreduce.conceptualschema;

import java.util.ArrayList;
import org.apache.spark.api.java.function.PairFunction;

import evaluator.mapreduce.measures.MValue;
import query.Query;
import scala.Tuple2;


public class PKVConstruction implements PairFunction<Tuple2<Key, ArrayList<MValue>>, Key, ArrayList<MValue>>{

	private Query query;
	
	public PKVConstruction(Query query) {
		this.query = query;
	}

	@Override
	public Tuple2<Key, ArrayList<MValue>> call(Tuple2<Key, ArrayList<MValue>> t) throws Exception {
		
		Key key = t._1;
		
		Key newKey = new Key();
		newKey.addAttributeValue(
				query.getQueryTriple()._1().getGroupingAttribute(), 
				key.getAttributeValue(query.getQueryTriple()._1().getGroupingAttribute()));
		
		return new Tuple2<>(newKey, t._2);
	}
}
