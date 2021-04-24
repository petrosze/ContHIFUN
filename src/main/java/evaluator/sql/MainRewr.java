package evaluator.sql;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.apache.spark.sql.streaming.StreamingQueryException;

import context.Context; 
import context.Synthetic;
import evaluator.InputQueries;
import evaluator.sql.execution.SQLExecutionModelRewr;
import query.QSet;
import query.Query;
import query.RSet;

public class MainRewr {

	private static final String TRIGGER_INTERVAL = "60 seconds";
	private static final String INPUT_FOLDER = "/paths/to/hdfs/datasets";
	
	
	public static void main(String[] args) throws StreamingQueryException {
		
		Context context = new Synthetic();
		InputQueries input_queries = new InputQueries();
		
		QSet qset = input_queries.getQsetCGMRR();
		//QSet qset = input_queries.getQsetCGRR(); 
		qset.createTriples();
		Map<Integer, Query> selectedQueries = qset.getSubset(Arrays.asList(1,2,3,4,5));
		RSet rset = new RSet(selectedQueries, qset.getRestrictions());
		
		if(rset.applyCommonGroupingMeasuringRule() == false) return;
		//if(rset.applyCommonGroupingRule() == false) return;
		
		HashSet<String> useful_attributes = rset.getUsefulAttributes();
		SQLExecutionModelRewr model = new SQLExecutionModelRewr(TRIGGER_INTERVAL, INPUT_FOLDER);
		model.setRSet(rset);
		model.setContext(context);
		model.setUsefulAttributes(useful_attributes);
		model.execute();
	}
}
