package evaluator.sql.execution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import context.Context;
import defines.RType;
import evaluator.sql.RowCreation;
import query.Query;
import query.RSet;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class BatchSQLExecutionModelRewr {
	
	private Context context; 
	private RSet rset;
	private HashSet<String> usefulAttributes;
	
	private ArrayList<String> datasets_paths;
	
	public BatchSQLExecutionModelRewr(ArrayList<String> paths) {
		this.datasets_paths = paths;
	}
	
	public void execute() throws InterruptedException { 
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkSession spark = SparkSession
				  .builder()
				  .appName("HiFun")
				  .master("local[*]")
				  .getOrCreate();
		
		Dataset<Row> df = spark
				.read()
				.text(convertListToSeq(datasets_paths));
				
		Dataset<Row> rows = df
				 .as(Encoders.STRING())
				 .flatMap(new RowCreation(context, usefulAttributes), createEncoder());
		
		if (rset.containsARestriction()) {
			rows = rows.where(rset.getARString());
		}
		
		Query baseQ = rset.getFirstQuery();
		
		if(rset.getRType() == RType.SAME_GROUPING_MEASURING_RULE) {
			rows = rows
					.groupBy(baseQ.getGroupingCols())
					.agg(baseQ.getExpr(), baseQ.getExprs());
		}
		else if(rset.getRType() == RType.SAME_GROUPING_RULE) {
			rows = rows
					.groupBy(baseQ.getGroupingCols())
					.agg(baseQ.getExprsMap());
		}
		rows.show();
		spark.close();
	}
	
	public void printInitializations() {
		System.out.println("~Batch SQL Execution Model~");
		System.out.println("------------");
		System.out.println("Query Set (RSet): " + this.rset.getRSetAsString());
		System.out.println("------------");
		System.out.println("Context Analysis \nSupporting Functions over Dataset: " + this.context.getFunctionsName());
		System.out.println("Useful Attributes including in RSet: " + this.usefulAttributes.toString());
	}
	
	public void setContext(Context invoices) {
		this.context = invoices;
	}

	public void setRSet(RSet rset) {
		this.rset = rset;
	}
	
	public void setUsefulAttributes(HashSet<String> useful_attributes) {
		this.usefulAttributes = new HashSet<String>(); 
		this.usefulAttributes = useful_attributes; 
	}

	private ExpressionEncoder<Row> createEncoder() {
		StructType structType = new StructType();
		for(String attr : usefulAttributes){
			structType = structType.add(attr, context.getTypes().get(attr), false);
		}
		return RowEncoder.apply(structType);
	}
	
	public Seq<String> convertListToSeq(List<String> inputList) {
	    return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}
}
