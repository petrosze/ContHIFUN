package evaluator.mapreduce.conceptualschema;

import java.util.ArrayList;
import java.util.ListIterator;

import org.apache.spark.api.java.function.Function2;

import defines.Operations;
import evaluator.mapreduce.measures.MValue;
import query.Query;
import queryparts.Grouping;
import queryparts.Measuring;
import queryparts.Operation;
import scala.Tuple3;

public class Reduction implements Function2<ArrayList<MValue>, ArrayList<MValue>, ArrayList<MValue>> {

	private Operation opPart; 
	
	public Reduction(Query query) {
		Tuple3<Grouping, Measuring, Operation> triple = query.getQueryTriple();
		opPart= triple._3();
	}

	@Override
	public ArrayList<MValue> call(ArrayList<MValue> val1, ArrayList<MValue> val2) throws Exception {
		
		ListIterator<MValue> it1 = val1.listIterator();
		ListIterator<MValue> it2 = val2.listIterator();
		ListIterator<Operations> opIt = opPart.getOperations().listIterator();
		
		while (it1.hasNext()) {
			it1.set(opPart.applyOperation(it1.next(), it2.next(), opIt.next()));
		}
		return val1;
	}
}
