package evaluator.sql;

import org.apache.spark.sql.streaming.StreamingQueryException;

import context.Context; 
import context.Synthetic;
import evaluator.InputQueries;
import evaluator.sql.execution.SQLExecutionModelNonRewr;
import query.QSet;

public class MainNonRewr {

	private static final String TRIGGER_INTERVAL = "60 seconds";
	private static final String INPUT_FOLDER = "/paths/to/hdfs/datasets";
	
	public static void main(String[] args) throws StreamingQueryException {

		Context context = new Synthetic();
		InputQueries input_queries = new InputQueries();
		
		QSet qset = input_queries.getQsetCGMRR();
		//QSet qset = input_queries.getQsetCGRR(); 
		qset.createTriples();
		
		SQLExecutionModelNonRewr model = new SQLExecutionModelNonRewr(TRIGGER_INTERVAL, INPUT_FOLDER);
		model.setQSet(qset);
		model.setContext(context);
		model.printInitializations();
		model.execute();
	}

}
