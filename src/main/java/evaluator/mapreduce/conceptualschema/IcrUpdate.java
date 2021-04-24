package evaluator.mapreduce.conceptualschema;

import java.util.ArrayList;
import java.util.ListIterator;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;

import defines.Operations;
import evaluator.mapreduce.measures.MValue;
import query.Query;
import queryparts.Grouping;
import queryparts.Measuring;
import queryparts.Operation;
import scala.Tuple2;
import scala.Tuple3;

public class IcrUpdate implements Function3<Key, Optional<ArrayList<MValue>>, State<ArrayList<MValue>>, Tuple2<Key, ArrayList<MValue>>> {
	
	private Operation opPart; 
	
	public IcrUpdate(Query query) {
		Tuple3<Grouping, Measuring, Operation> triple = query.getQueryTriple();
		this.opPart = triple._3();
	}

	@Override
	public Tuple2<Key, ArrayList<MValue>> call( Key key, 
												Optional<ArrayList<MValue>> newValue, 
												State<ArrayList<MValue>> oldValue) throws Exception {
		
		ArrayList<MValue> updated = new ArrayList<>();
		if(oldValue.exists()) {
			ListIterator<MValue> newVal = newValue.get().listIterator();
			ListIterator<MValue> oldVal = oldValue.get().listIterator();
			ListIterator<Operations> opIt = opPart.getOperations().listIterator();
			while (opIt.hasNext()) {
				MValue result = opPart.applyOperation(newVal.next(), oldVal.next(), opIt.next());
				updated.add(result);
			}
		}
		else {
			updated = newValue.get();
		}
		oldValue.update(updated);
		Tuple2<Key, ArrayList<MValue>> output = new Tuple2<>(key, updated);
		return output;
	}
}
