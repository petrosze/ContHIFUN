import query.QSet;

public class InputQueries {
	
	private QSet qsetCGMRR = new QSet();
	private QSet qsetCGRR = new QSet(); 
	private QSet qsetCMORR = new QSet();
	private QSet qsetBRR = new QSet(); 
	
	public InputQueries() {
		
		// Common Grouping and Measuring Rewriting Rule
		qsetCGMRR.addCQuery("(g1, m1, sum)"); 
		qsetCGMRR.addCQuery("(g1, m1, min)"); 
		qsetCGMRR.addCQuery("(g1, m1, max)"); 
		qsetCGMRR.addCQuery("(g1, m1, count)"); 
		qsetCGMRR.addCQuery("(g1, m1, avg)"); 
		
		// Common Grouping Rewriting Rule
		qsetCGRR.addCQuery("(g1, m1, sum)"); 
		qsetCGRR.addCQuery("(g1, m2, min)"); 
		qsetCGRR.addCQuery("(g1, m3, max)"); 
		qsetCGRR.addCQuery("(g1, m4, count)"); 
		qsetCGRR.addCQuery("(g1, m5, avg)"); 
		
		// Common Measuring and Operation Rewriting Rule
		qsetCMORR.addCQuery("(g1, m1, sum)"); 
		qsetCMORR.addCQuery("(g2, m1, sum)"); 
		qsetCMORR.addCQuery("(g3, m1, sum)"); 
		qsetCMORR.addCQuery("(g4, m1, sum)"); 
		qsetCMORR.addCQuery("(g5, m1, sum)"); 
		
		// Basic Rewriting Rule
		qsetBRR.addCQuery("(g11°g1, m1, sum)"); 
		qsetBRR.addCQuery("(g12°g1, m1, sum)"); 
		qsetBRR.addCQuery("(g13°g1, m1, sum)"); 
		qsetBRR.addCQuery("(g14°g1, m1, sum)"); 
		qsetBRR.addCQuery("(g15°g1, m1, sum)"); 
	}

	public QSet getQsetCGMRR() {
		return qsetCGMRR;
	}

	public QSet getQsetCGRR() {
		return qsetCGRR;
	}

	public QSet getQsetCMORR() {
		return qsetCMORR;
	}

	public QSet getQsetBRR() {
		return qsetBRR;
	}
	
	
}
