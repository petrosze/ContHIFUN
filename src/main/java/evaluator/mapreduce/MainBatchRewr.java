package evaluator.mapreduce;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import context.Context; 
import context.Synthetic;
import evaluator.InputQueries;
import evaluator.mapreduce.execution.BatchMapReduceExecutionModelRewr;
import query.QSet;
import query.Query;
import query.RSet;
import utils.ReadPathsFromLocalFile;

public class MainBatchRewr {

	private static ArrayList<String> datasets_paths = null; 
	
	public static void main(String[] args) throws InterruptedException, FileNotFoundException {
		
		datasets_paths = new ArrayList<String>(); 
		if(datasets_paths == null) 
			datasets_paths = new ReadPathsFromLocalFile().getPaths();
		
		Context context = new Synthetic();
		InputQueries input_queries = new InputQueries();
		
		QSet qset = input_queries.getQsetCGMRR();
		//QSet qset = input_queries.getQsetCGRR();
		//QSet qset = input_queries.getQsetCMORR();
		//QSet qset = input_queries.getQsetBRR(); 
		qset.createTriples();
		 
		
		Map<Integer, Query> selectedQueries = qset.getSubset(Arrays.asList(1,2,3,4,5));
		RSet rset = new RSet(selectedQueries, qset.getRestrictions());
		if(rset.applyCommonGroupingMeasuringRule() == false) return;
		//if(rset.applyCommonGroupingRule() == false) return;
		//if(rset.applyCommonMeauringOperationRule() == false) return;
		//if(rset.applyBasicRewritingRule() == false) return;
		
		HashSet<String> useful_attributes = rset.getUsefulAttributes();
		BatchMapReduceExecutionModelRewr model = new BatchMapReduceExecutionModelRewr(datasets_paths);
		model.setContext(context);
		model.setRSet(rset);
		model.setUsefulAttributes(useful_attributes);
		model.execute();
	}
}
