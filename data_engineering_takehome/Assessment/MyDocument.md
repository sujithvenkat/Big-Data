Learnings:

	Learned how to process large datasets using PySpark.
	Applied window functions for ranking (highest salary per agency).
	Understood how feature engineering improves analysis.
	Practiced storing processed data in Parquet format.
	
Challenges

	Handling Spark toPandas() errors due to file caching.
	Managing datatype issues (Decimal vs Double).
	Designing clean separation between transformation and visualization.
	identify the value differeces between spark 2.4.5 to spark 3
	
Assumptions

	Salary ranges used midpoint approximation
	Degree extracted via keyword matching
	Skills extracted from unstructured text
	Dataset historical until 2019
	
Deployment Idea
	
	Package the PySpark script as a batch job.
	Schedule using Airflow or Cron.
	Store processed data in S3 or HDFS.
	Connect visualization to BI tool (Power BI / Tableau).

Trigger Approach
	
	Job can be triggered manually using spark-submit.
	Can also be scheduled daily/weekly using workflow orchestrator.
