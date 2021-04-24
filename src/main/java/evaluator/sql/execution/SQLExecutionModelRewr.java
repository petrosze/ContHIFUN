package evaluator.sql.execution;
 
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession; 
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Level;

import context.Context;
import defines.RType;
import query.Query;
import query.RSet;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;


public class SQLExecutionModelRewr implements Serializable{

	private String TRIGGER_INTERVAL;
	private String INPUT_FOLDER;
	private ArrayList<String> usefulAttributes;
	
	private Context context; 
	private RSet rset;
	
	public SQLExecutionModelRewr(String _TRIGGER_INTERVAL, String _INPUT_FOLDER) {
		this.TRIGGER_INTERVAL = _TRIGGER_INTERVAL;
		this.INPUT_FOLDER = _INPUT_FOLDER; 
	}
	
	public void execute() throws StreamingQueryException {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName("HiFun")
				.getOrCreate();

		Dataset<Row> rows = spark
				.readStream()
				.option("header", "false")
			    .option("maxFilesPerTrigger",1)
				.schema(context.getSchema())
				.csv(INPUT_FOLDER)
				.select(convertListToCols(this.usefulAttributes)); //construction
			
		if (rset.containsARestriction()) {
			rows = rows.where(rset.getARString());
		}
		
		Query firstQuery = rset.getFirstQuery();
			
		if(rset.getRType() == RType.SAME_GROUPING_MEASURING_RULE) {
			rows = rows
					.groupBy(firstQuery.getGroupingCols())
					.agg(firstQuery.getExpr(), firstQuery.getExprs());
		}
		else if(rset.getRType() == RType.SAME_GROUPING_RULE) {
			rows = rows
					.groupBy(firstQuery.getGroupingCols())
					.agg(firstQuery.getExprsMap());
		}
		
		StreamingQuery query = rows
				.writeStream()
				.outputMode(OutputMode.Update())
				.format("console")
				.trigger(Trigger.ProcessingTime(TRIGGER_INTERVAL))
				.start();
		
		query.awaitTermination();
	}
	
	public void printInitializations() {
		System.out.println("~SQL Execution Model~");
		System.out.println("Trigger Interval: " + this.TRIGGER_INTERVAL);
		
		System.out.println("Input Folder: " + this.INPUT_FOLDER );
		System.out.println("(the folder contains the input files which is the simulation of input stream");
		
		System.out.println( "------------\n" + "Query Set (RSet): " + this.rset.getRSetAsString() + "------------"); 
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
		this.usefulAttributes = new ArrayList<String>(useful_attributes);  
	}
	
	private Buffer<Column> convertListToCols(ArrayList<String> atributes) {
		List<Column> cols_l = new ArrayList<Column>();  			
		for(String attr : atributes) {
			cols_l.add(new Column(attr));
		}
		return JavaConversions.asScalaBuffer(cols_l);
	}
}
