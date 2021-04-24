# ContHIFUN

Incremental Continuous Queries Evaluation in HIFUN  <br />
HIFUN is a high-level query languege for expressing analytic queries over big data sets. In this project, based on HIFUN languege we implement the corresponding algorithms for the incremental evaluation of continuous queries. We translate HIFUN queries to SQL and MapReduce tasks using Spark Streaming and Spark Structured Streaming, supporting also various queries rewriting methods proposed by HIFUN. 

Datasets
A synthetic dataset used split into 10 files and pushed into a queue, treaded as a batch of data and processed like a stream. To distributed the data uniformly among all the cluster works, the data follows uniform distribution and stored in distributed file system. In case of the map reduce execution model the source data set is provided as a single text file and the analysis context used to extract values to formulate queries (in the form of triples as proposed of HIFUN theory), whereas in the case of SQL execution model, the source data set is provided as a structure dataset according to relational schema. 

Queries
Common Grouping Measuring Rewriting Rule (CGMRR)
Queries used without rewritings
Q1 = (g1, m1, sum)
Q2 = (g1, m1, min)
Q3 = (g1, m1, max)
Q4 = (g1, m1, count)
Q5 = (g1, m1, avg)
Query after rewriting 
Q = (g1, m1, {sum, min, max, count, avg})

Common Grouping Rewriting Rule (CGRR)
Queries used without rewriting
Q1 = (g1, m1, sum)
Q2 = (g1, m2, min)
Q3 = (g1, m3, max)
Q4 = (g1, m4, count)
Q5 = (g1, m5, avg)

Query after rewriting
Q = (g1, {m1, sum}, {m2, min}, {m3, max}, {m5, avg})

Common Measuring and Operation Rewriting Rule (CMORR)
Queries used without rewriting
Q1 = (g1, m1, sum)
Q2 = (g2, m1, sum)
Q3 = (g3, m1, sum)
Q4 = (g4, m1, sum)
Q5 = (g5, m1, sum)

Queries after rewritings
Q1 = ((g1 ^ g2 ^ g3 ^ g4 ^ g5), m1, sum)
Q2 = (projG1, Q1, sum)
Q3 = (projG2, Q1, sum)
Q4 = (projG3, Q1, sum)
Q5 = (projG4, Q1, sum)
Q6 = (projG5, Q1, sum)

Basic Rewriting Rule (BRR)
Queries used without rewriting
Q1 = (g11°g1, m1, sum)
Q2 = (g12°g1, m1, sum)
Q3 = (g13°g1, m1, sum)
Q4 = (g14°g1, m1, sum)
Q5 = (g15°g1, m1, sum)

Queries after rewriting
Q1 = (g1, m, sum)
Q2 = (g11, Q1, sum)
Q3 = (g12, Q1, sum)
Q4 = (g13, Q1, sum)
Q5 = (g14, Q1, sum)
Q6 = (g15, Q1, sum)

References. 
[1] Spyratos, N.; Sugibuchi, T. A High Level Query Language for Big Data Analytics.  2014.
[2] Spyratos, N.; Sugibuchi, T.  HIFUN - a high level functional query language for big data analytics. Journal of Intelligent Information Systems 2018,51, 529–555.718
[3] Zervoudakis, P.; Kondylakis, H.; Plexousakis, D.; Spyratos, N.  Incremental Evaluation of Continuous Analytic Queries in HIFUN. ISIP, 2019, pp. 53–67.
