package evaluator.mapreduce.conceptualschema;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFunction;

import defines.GType;
import defines.Operations;
import evaluator.mapreduce.measures.AvgMValue;
import evaluator.mapreduce.measures.MValue;
import evaluator.mapreduce.measures.SingleMValue;
import query.Query;
import queryparts.Grouping;
import queryparts.Measuring;
import queryparts.Operation;
import scala.Tuple2;
import scala.Tuple3;

public class KVConstruction implements PairFunction<Tuple, Key, ArrayList<MValue>> {

	private Grouping gPart;
	private Measuring mPart;
	private Operation opPart; 
	
	public KVConstruction(Query query) {
		Tuple3<Grouping, Measuring, Operation> triple = query.getQueryTriple();
		gPart = triple._1();
		mPart = triple._2();
		opPart= triple._3();
	}
	
	@Override
	public Tuple2<Key, ArrayList<MValue>> call(Tuple tuple) throws Exception {
		
		ArrayList<MValue> values = new ArrayList<>();
		int mIndex = 0;
		int i = 0;
		
		for (Operations operation : opPart.getOperations()) {
			
			if (mPart.getMeasuringAttributes().size() > 1) {
				mIndex = i;
			}
			MValue mvalue; 
			if(operation == Operations.AVG) {
				mvalue = new AvgMValue();
				mvalue.setMValue(tuple.getAttributeValue(mPart.getMeasuringAttributes().get(mIndex)));
			}
			else if(operation == Operations.COUNT) {
				mvalue = new SingleMValue();
				mvalue.setMValue(new Integer(1));
			}
			else {
				mvalue = new SingleMValue();
				mvalue.setMValue(tuple.getAttributeValue(mPart.getMeasuringAttributes().get(mIndex)));
			}
			values.add(mvalue);
			i++;
		}
		
		return new Tuple2<>(createKey(tuple), values);
	}

	private Key createKey(Tuple t) {
		
		GType type = gPart.getGroupingType();
		Key key = new Key();
		if(type == GType.SIMPLE_GROUPING) {
			key.addAttributeValue(gPart.getGroupingAttribute(), (String) t.getAttributeValue(gPart.getGroupingAttribute()));
		}
		else if(type == GType.CARTESIAN_GROUPING) {
			for(String gName : gPart.getGroupingAttributes()) {
				key.addAttributeValue(gName, (String)t.getAttributeValue(gName) );
			}
		}
		else if(type == GType.COMPOSITION_GROUPING) {
			String gname = gPart.getCompGroupingAsString();
			key.addAttributeValue(gname, (String)t.getAttributeValue(gname));
		}
		return key;
	}
	
	@SuppressWarnings("unused")
	private String createKeyAsString(Tuple t) {
		
		GType type = gPart.getGroupingType();
		if(type == GType.SIMPLE_GROUPING) {
			return (String) (t.getAttributeValue(gPart.getGroupingAttribute()));
		}
		else if(type == GType.CARTESIAN_GROUPING) {
			StringBuilder str = new StringBuilder();
			String prefix = "";
			for(String gName : gPart.getGroupingAttributes()) {
				str.append(prefix);
				prefix = ",";
				str.append(t.getAttributeValue(gName));
			}
			return str.toString();
		}
		else if(type == GType.COMPOSITION_GROUPING) { 
			String cAttr = "";
			String prefix ="";
			for(String gName : gPart.getGroupingAttributes()) {
				cAttr += prefix;
				prefix = "*";
				cAttr += gName;
			}
			return (String)(t.getAttributeValue(cAttr));
		}
		return "";
	}
}
