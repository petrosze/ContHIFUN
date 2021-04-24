package query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import defines.Operations;
import queryparts.Grouping;
import queryparts.Measuring;
import queryparts.Operation;
import scala.Tuple3;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

public class Query implements Serializable{

	private String cquery;
	
	private Grouping gPart;
	private Measuring mPart;
	private Operation op;

	public Query(String cquery) {
		this.cquery = cquery;
	}

	public Query(Tuple3<Grouping, Measuring, Operation> t) {
		this.cquery = null; /* never used in this case */
		this.gPart = t._1();
		this.mPart = t._2();
		this.op = t._3();
	}

	public void createTriple() {
		Tuple3<String, String, String> parts = getParts();
		this.gPart = new Grouping(parts._1());
		this.mPart = new Measuring(parts._2());
		this.op    = new Operation(strToOperation(parts._3()));
	}

	public String getCQuery() {
		return this.cquery;
	}
	

	public Tuple3<Grouping, Measuring, Operation> getQueryTriple(){
		return new Tuple3<>(this.gPart, this.mPart, this.op);
	}

	@Override
	public String toString() {
		return "Query [gPart=" + gPart + ", mPart=" + mPart + ", op=" + op + "]";
	}
	
	public Buffer<Column> getGroupingCols() {
		List<Column> grouping = new ArrayList<Column>(); 
		for(String gr : this.gPart.getGroupingAttributes()) {
			grouping.add(new Column(gr));
		}
		return JavaConversions.asScalaBuffer(grouping);
	}
	
	public Map<String, String> getExprsMap() {
		Map<String, String> exprs = new HashMap<String, String>();
		int opIdx = 0;
		for(String m : mPart.getMeasuringAttributes()) {
			exprs.put(m, op.getOperations().get(opIdx).name());
			opIdx++;
		}
		return exprs;
	}

	// ....
	public Column getExpr() {
		Column expr = createExpr(op.getOperation(), mPart.getMeasuringAttribute());
		return expr;
	}

	// ... ....
	public Buffer<Column> getExprs() {
		
		String m = mPart.getMeasuringAttribute();
		List<Operations> opl = op.getOperations();
		List<Column> exprs = new ArrayList<Column>();  
		
		for(int index=1; index<opl.size(); index++) {
			exprs.add(createExpr(opl.get(index), m));
		}
		return JavaConversions.asScalaBuffer(exprs);
	}
	
	// .....!!!!!
	private Column createExpr(Operations op, String colName) {
		if(op == Operations.MIN)
			return functions.min(colName);
		if(op == Operations.MAX)
			return functions.max(colName);
		if(op == Operations.COUNT)
			return functions.count(colName);
		if(op == Operations.SUM)
			return functions.sum(colName);
		if(op == Operations.AVG)
			return functions.avg(colName);
		else
			return null;
	}

	
	private Tuple3<String, String, String> getParts() {
		String query = cquery.replace("(", "").replace(")", "").replaceAll(" ", "");
		String [] parts = query.split(",");
		return new Tuple3<>(parts[0], parts[1], parts[2]); 
	}
	
	private Operations strToOperation(String op) {
		switch (op) {
			case "min":
				return Operations.MIN;
			case "max":
				return Operations.MAX;
			case "count":
				return Operations.COUNT;
			case "sum":
				return Operations.SUM;
			case "avg":
				return Operations.AVG;
			default: 
				return Operations.NON;
			}
	}

}
