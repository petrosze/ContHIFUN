package evaluator.sql;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map; 
import context.Context; 
import context.Synthetic;
import evaluator.InputQueries;
import evaluator.sql.execution.BatchSQLExecutionModelRewr;
import query.QSet;
import query.Query;
import query.RSet;
import utils.ReadPathsFromLocalFile;

public class MainBatchRewr {

	private static ArrayList<String> datasets_paths = new ArrayList<String>(); 
	
	public static void main(String[] args) throws InterruptedException, FileNotFoundException {
		
		if(datasets_paths == null) 
			datasets_paths = new ReadPathsFromLocalFile().getPaths();
	
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
		BatchSQLExecutionModelRewr model = new BatchSQLExecutionModelRewr(datasets_paths);
		model.setContext(context);
		model.setRSet(rset);
		model.setUsefulAttributes(useful_attributes);
		//model.printInitializations();
		model.execute();
		 
	}
}
