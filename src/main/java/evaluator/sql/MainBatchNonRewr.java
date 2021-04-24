package evaluator.sql;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;

import context.Context;
import context.Synthetic;
import evaluator.InputQueries;
import evaluator.sql.execution.BatchSQLExecutionModelNonRewr;
import query.QSet;
import utils.ReadPathsFromLocalFile;

public class MainBatchNonRewr {

	private static ArrayList<String> datasets_paths = null; 
	
	public static void main(String[] args) throws InterruptedException, FileNotFoundException {
			
		if(datasets_paths == null) 
			datasets_paths = new ReadPathsFromLocalFile().getPaths();
		
		Context context = new Synthetic();
		InputQueries input_queries = new InputQueries();
		
		QSet qset = input_queries.getQsetCGMRR();
		//QSet qset = input_queries.getQsetCGRR();
		qset.createTriples();
		
		HashSet<String> useful_attributes = qset.getUsefulAttributes();
		BatchSQLExecutionModelNonRewr model = new BatchSQLExecutionModelNonRewr(datasets_paths);
		model.setContext(context);
		model.setQSet(qset);
		model.setUsefulAttributes(useful_attributes);
		//model.printInitializations(); 
		model.execute();
	}
}
