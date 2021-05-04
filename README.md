# ContHIFUN

Incremental Continuous Queries Evaluation in HIFUN  <br />
HIFUN is a high-level query language for expressing analytic queries over big data sets. In this project, based on HIFUN language we implement the corresponding algorithms for the incremental evaluation of continuous queries. We translate HIFUN queries to SQL and MapReduce tasks using Spark Streaming and Spark Structured Streaming, supporting also various queries rewriting methods proposed by HIFUN. 

Datasets <br />
A synthetic dataset used split into 10 files and pushed into a queue, treaded as a batch of data and processed like a stream. To distributed the data uniformly among all the cluster works, the data follows uniform distribution and stored in distributed file system. In case of the map reduce execution model the source data set is provided as a single text file and the analysis context used to extract values to formulate queries (in the form of triples as proposed of HIFUN theory), whereas in the case of SQL execution model, the source data set is provided as a structure dataset according to relational schema. 

Queries <br />
Common Grouping Measuring Rewriting Rule (CGMRR) <br />
Queries used without rewritings  <br />
Q1 = (g1, m1, sum)  <br />
Q2 = (g1, m1, min)  <br />
Q3 = (g1, m1, max)  <br />
Q4 = (g1, m1, count)  <br />
Q5 = (g1, m1, avg)  <br />
Query after rewriting  <br />
Q = (g1, m1, {sum, min, max, count, avg}) <br />

Common Grouping Rewriting Rule (CGRR) <br />
Queries used without rewriting <br />
Q1 = (g1, m1, sum) <br />
Q2 = (g1, m2, min) <br />
Q3 = (g1, m3, max) <br />
Q4 = (g1, m4, count) <br />
Q5 = (g1, m5, avg) <br />
 <br />
Query after rewriting <br />
Q = (g1, {m1, sum}, {m2, min}, {m3, max}, {m5, avg}) <br />
 <br />
Common Measuring and Operation Rewriting Rule (CMORR) <br />
Queries used without rewriting <br />
Q1 = (g1, m1, sum) <br />
Q2 = (g2, m1, sum) <br />
Q3 = (g3, m1, sum) <br />
Q4 = (g4, m1, sum) <br />
Q5 = (g5, m1, sum) <br />
 <br />
Queries after rewritings <br />
Q1 = ((g1 ^ g2 ^ g3 ^ g4 ^ g5), m1, sum) <br />
Q2 = (projG1, Q1, sum) <br />
Q3 = (projG2, Q1, sum) <br />
Q4 = (projG3, Q1, sum) <br />
Q5 = (projG4, Q1, sum) <br />
Q6 = (projG5, Q1, sum) <br />
 <br />
Basic Rewriting Rule (BRR) <br />
Queries used without rewriting <br />
Q1 = (g11°g1, m1, sum) <br />
Q2 = (g12°g1, m1, sum) <br />
Q3 = (g13°g1, m1, sum) <br />
Q4 = (g14°g1, m1, sum) <br />
Q5 = (g15°g1, m1, sum) <br />
 <br />
Queries after rewriting <br />
Q1 = (g1, m, sum) <br />
Q2 = (g11, Q1, sum) <br />
Q3 = (g12, Q1, sum) <br />
Q4 = (g13, Q1, sum) <br />
Q5 = (g14, Q1, sum) <br />
Q6 = (g15, Q1, sum) <br />
 <br />
 
Launching the project with spark-submit <br />

Once an experiment configuration is bundled, it can be launched using the bin/spark-summit script and runs on Mesos cluster. This script takes care of setting up the classpath with Spark and its dependencies and can support different cluster configutations and deploy modes.  <br />

 <br />
spark-submit \ <br />
--class src.main.java.evaluator.mapreduce.MainRewr \ <br />
--driver-memory 20G \ <br />
--executor-memory 200G \ <br />
--conf spark.speculation=true \ <br />
--master mesos://zk://clusternode5:2181,clusternode6:2181,clusternode7:2181/mesos \ <br />
hifun.jar <br />

References. <br /> 
[1] Spyratos, N.; Sugibuchi, T. A High Level Query Language for Big Data Analytics.  2014. <br />
[2] Spyratos, N.; Sugibuchi, T.  HIFUN - a high level functional query language for big data analytics. Journal of Intelligent Information Systems 2018,51, 529–555.718 <br />
[3] Zervoudakis, P.; Kondylakis, H.; Plexousakis, D.; Spyratos, N.  Incremental Evaluation of Continuous Analytic Queries in HIFUN. ISIP, 2019, pp. 53–67. <br />
