package defines;

/**
 * All supporting grouping operators
 * 
 * */

public class GOperator {

	public static final String CARTESIAN_PRODUCT = "^"; /*Query Example: Q=(g1^g2, m, op)*/
	public static final String COMPOSITION = "*";		/*Query Example: Q=(g1°g2, m, op)*/
	public static final String PROJECTION = "proj_";    /*Query Example: Q=(proj_F,m,op)*/
}
