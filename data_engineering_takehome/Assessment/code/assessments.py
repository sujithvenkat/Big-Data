import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import col, sum, when, countDistinct, to_timestamp, to_date, add_months, lower
from pyspark.ml.feature import StopWordsRemover, Tokenizer, NGram

def load_data(spark, read_path):
    "Data Munging: Raw Data Discovery"
    '''
    The source CSV contained embedded commas and line breaks within quoted text fields (e.g., job descriptions). 
    To prevent incorrect parsing and column misalignment, multiline parsing with quote and escape handling was enabled.
    '''
    df=spark.read\
        .option("header", "true")\
        .option("inferSchema","false")\
        .option("quote",'"')\
        .option("multiline","true")\
        .option("escape",'\"')\
        .csv(read_path)
    df.printSchema()
    return df

def data_cleaning(df):
    "Data Preparation"
    '''
    Data Exploration was performed on raw data to understand structure and quality. Data Processing steps were derived 
    from exploration insights to ensure informed transformations.
    '''
    total_rows = df.count()  # 2946
    print("Total rows : ", total_rows)
    "Null Counts"
    null_counts = df.select([
        sum(when(col(c).isNull() | (col(c) == ''),1).otherwise(0)).alias(c)
        for c in df.columns])

    rows=null_counts.first().asDict()
    cols_with_nulls = [k for k, v in rows.items() if v > 0]
    nulls_threshold = [k for k,v in rows.items() if (v/total_rows) > 0.3]

    print("Columns with NULL values:")
    print(cols_with_nulls)

    print("\nColumns with NULL threshold > 30%:")
    print(nulls_threshold)

    '''
    NULL values in textual columns (e.g., Preferred Skills, Minimum Qual Requirements) were preserved due to lack of 
    business rules for imputation and to avoid loss of information.
    '''
    "As we are tokenizing the preferred skills in the down lets replace null values with spaces as tokenizer can't accept NULLS"
    df = df.withColumn(
        "preferred_skills_clean",
        F.coalesce(F.col("Preferred Skills"), F.lit(""))
    )
    return df

def data_pre_processing(df):
    "Data Wrangling"
    '''While exploring the dataset, I observed the presence of unwanted Unicode/encoding artifacts (e.g., special bullet symbols 
    and mis-encoded characters such as Ã¢â‚¬Â¢) in the Preferred Skills column. These characters likely originated from encoding 
    inconsistencies during CSV ingestion.
    As part of the data cleaning process, I applied a regular expression transformation to remove non-alphanumeric characters 
    before performing text analysis. This ensures consistent tokenization and accurate skill extraction.'''

    # Convert to lowercase for uniform processing
    '''To preserve raw data integrity, transformations were applied to a new derived column (preferred_skills_clean) 
    rather than modifying the original field.'''

    df = df.withColumn("preferred_skills_clean",F.lower(F.col("preferred_skills_clean")))
    # Remove non-alphanumeric characters (including encoding artifacts)
    df = df.withColumn("preferred_skills_clean",F.regexp_replace("preferred_skills_clean", "[^a-z0-9 ]", " "))

    "Column profiling -- Distinct counts helped identify categorical vs high-cardinality columns"

    print("\nColumns with their Distinct counts:")
    df.select([
        countDistinct(col(c)).alias(c) for c in df.columns]
    ).show(truncate=False)

    "With the above we can understand the low cardinal columns - categorical columns etc and high cardinal columns - IDs, descriptions etc"
    "Data Preparation: Data Cleaning"

    "Data Enrichment - casting"
    # Cast numeric columns
    df = df.withColumn("# Of Positions",col("# Of Positions").cast("int"))\
        .withColumn("Salary Range From", col("Salary Range From").cast("double"))\
        .withColumn("Salary Range To", col("Salary Range To").cast("double"))
    #Data Cleaning
    df = df.filter(col("Salary Range From").isNotNull() & (col("Salary Range From") > 0))\
        .filter(col("Salary Range To").isNotNull() & (col("Salary Range To") > 0))

    # Convert date columns
    fmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    df = df.withColumn(
        "Post Until",
        when(col("Post Until") == "", None).otherwise(col("Post Until"))
    )

    df = df.withColumn("Posting Date", to_timestamp("Posting Date", fmt)) \
           .withColumn("Post Until", to_timestamp("Post Until", fmt)) \
           .withColumn("Posting Updated", to_timestamp("Posting Updated", fmt)) \
           .withColumn("Process Date", to_timestamp("Process Date", fmt))

    df = df.withColumn("Posting Date", to_date("Posting Date")) \
           .withColumn("Post Until", to_date("Post Until")) \
           .withColumn("Posting Updated", to_date("Posting Updated")) \
           .withColumn("Process Date", to_date("Process Date"))

    df.printSchema()
    '''Date columns stored as ISO timestamp strings were converted to timestamp and then to date type to ensure accurate parsing " \
    while preserving NULL values.'''

    "Summary statistics for numeric columns"
    print("\nSummary statistics for numeric columns")
    df.describe().select("Summary","Salary Range From", "Salary Range To").show()

    "Top values in categorical columns"
    print("\nTop values in categorical columns:")
    df.groupBy("Job Category").count().orderBy("count", ascending= False).show(10, False)

    return df

def data_normalization(df):
    "Data processing and transformation"
    print("\nQuestion 1: Whats the number of jobs posting per category (Top 10)?")

    top_10_jobs_cnt_per_category = df.groupBy("Job Category").agg(F.count("*").alias("Number of Jobs")).orderBy(col("Number of Jobs").desc()).limit(10)
    top_10_jobs_cnt_per_category.show(10,False)

    print("\nQuestion 2:Whats the salary distribution per job category?")

    '''Typical assumptions :
    Hourly → 2080 hours/year (40 hrs/week * 52 weeks)
    Daily  → 260 days/year (5 days/week * 52 weeks)
    Annual → same
    '''

    "Salary ranges were first normalized to annual compensation using Salary Frequency to ensure fair comparison across agencies."

    df = df.withColumn("Annual_sal_from",when(col("Salary Frequency") == "Annual", col("Salary Range From"))
                                .when(col("Salary Frequency") == "Daily", col("Salary Range From") * 5 * 52)
                                .when(col("Salary Frequency") == "Hourly", col("Salary Range From") * 40 * 52)
                                .otherwise(None)).\
        withColumn("Annual_sal_to",when(col("Salary Frequency") == "Annual", col("Salary Range To"))
                                .when(col("Salary Frequency") == "Daily", col("Salary Range To") * 5 * 52)
                                .when(col("Salary Frequency") == "Hourly", col("Salary Range To") * 40 * 52)
                                .otherwise(None))

    "Salary midpoint used as average salary for the job, as it better represents typical compensation than the upper bound alone."
    df = df.withColumn("Salary_Mid", (col("Annual_sal_from") + col("Annual_sal_to"))/2)
    df.cache() # cache the dataframe from here we are going to do major transformation
    df = df.withColumn("Job_Category_Array", F.split(col("Job Category"), ",\\s*"))
    df = df.withColumn("Job_Category_Exploded", F.explode("Job_Category_Array"))
    sal_dist_per_cat = df.groupBy("Job_Category_Exploded")\
        .agg(
            F.min("Salary_Mid").alias("min_salary"),
            F.max("Salary_Mid").alias("max_salary"),
            F.avg("Salary_Mid").alias("avg_salary")
    )
    sal_dist_per_cat.show(10, False)

    print("\nQuestion 3: Is there any correlation between the higher degree and the salary?")

    '''Since the dataset does not contain a structured degree column, I would extract degree requirements from the Minimum 
    Qual Requirements text using keyword matching or regex. Then encode degree levels numerically to compute correlation 
    with salary. we are checking highest degree first to avoid picking High School if a posting contains both
    bachelor and High school'''

    df = df.withColumn("qualification_text", lower(col("Minimum Qual Requirements")))
    df = df.withColumn(
        "Min_Degree",
        when(col("qualification_text").contains("master"), "Master")
        .when(col("qualification_text").contains("baccalaureate") |
              col("qualification_text").contains("bachelor"), "Bachelor")
        .when(col("qualification_text").contains("associate degree"), "Associate")
        .when(col("qualification_text").contains("high school"), "High School")
        .otherwise("Not Specified")
    )
    df.groupBy("Min_Degree").count().show()
    '''
    Since degree level is an ordinal categorical variable, applying direct numerical correlation may introduce artificial 
    spacing assumptions between levels. Therefore, instead of computing correlation, salaries were compared across degree 
    groups to identify trends between education level and compensation.
    '''

    df_corr=df.groupBy("Min_Degree").agg(F.avg("Salary_Mid").alias("avg_sal_mid")).orderBy(F.col("avg_sal_mid").desc())
    df_corr.show()

    '''
    The analysis shows a general upward trend in average salary with increasing degree requirements, indicating a positive relationship 
    between higher education and compensation.
    '''

    print("\nQuestion 4: Whats the job posting having the highest salary per agency?")

    w=Window.partitionBy("Agency").orderBy(col("Salary_Mid").desc())
    highest_sal_per_agency=df.withColumn("rw",F.row_number().over(w)).filter(col("rw") == 1).drop("rw").select("Agency","Business Title","Salary_Mid")
    highest_sal_per_agency.show(10, False)

    print("\nQuestion 5: Whats the job positings average salary per agency for the last 2 years?")

    '''The dataset contains historical data (up to 2019). Therefore, the last 2 years were calculated relative to the maximum 
    posting date in the dataset rather than the system date.'''
    Max_Posting_Date = df.agg(F.max("Posting Date")).first()[0]
    filtered_df = df.where(col("Posting Date") >= F.add_months(F.lit(Max_Posting_Date), -24))

    avg_sal_agency = (
        filtered_df.groupBy("Agency")
        .agg(F.avg("Salary_Mid").alias("avg_salary_last_2yrs"))
        .orderBy(F.col("avg_salary_last_2yrs").desc())
    )
    avg_sal_agency.show(truncate=False)

    print("\nQuestion 6: What are the highest paid skills in the US market?")

    '''Since skills were provided as unstructured text, I performed text normalization and tokenization on the Preferred Skills 
    column, removed noise terms, and computed word frequencies. I then associated average salary with each skill keyword to 
    identify the highest paid skills.'''

    # Create tokenizer object to the cleaned skills columns to get array of words
    tokenizer= Tokenizer(inputCol="preferred_skills_clean", outputCol="words")
    df_tokenized = tokenizer.transform(df)
    #df_tokenized.show(10, False)
    #Built in stopwords remover
    default_stopwords = StopWordsRemover.loadDefaultStopWords("english")
    '''Based on frequency analysis of tokenized text, additional domain-specific stopwords were identified and removed
    to eliminate generic job-description terms. This improved the accuracy of skill extraction before computing average
    salary per skill.'''
    custom_stopwords = ["experience","skills","skill","ability","knowledge",
    "strong","excellent","work","working","years","year","preferred","including",
    "written","communication","management","new","required","must","state","time",
    "detail","using","wide","organizational","verbal","interpersonal","proficiency",
    "multiple","public","project","projects","systems","system","office","city",
    "word","york","data","candidate","candidates","demonstrated","related","plus",
    "analytical","team","oral","well","familiarity","development","technical",
    "improvement","success","goals","continuous","managerial","leading","leader",
    "implement","overseeing","environment"]
    all_stopwords = default_stopwords + custom_stopwords
    remover= StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=all_stopwords)
    df_filtered=remover.transform(df_tokenized)
    #df_filtered.show(10, False)
    #Explode filtered words into rows
    tech_skills = [
        "python", "sql", "aws", "azure", "spark",
        "excel", "powerbi", "tableau",
        "kubernetes", "docker",
        "devops", "linux", "java",
        "cloud", "security"
    ]
    ngram = NGram(n=2, inputCol="filtered_words", outputCol="bigrams")
    df_ngram = ngram.transform(df_filtered)
    df_exploded=df_filtered.select("Salary_Mid", F.explode("filtered_words").alias("word")).\
    filter(
        (F.col("word").isNotNull()) &
        (F.col("word") != "") &
        (F.length("word") > 2) &
        (~F.col("word").rlike("^[0-9]+$"))
    )
    df_exploded = df_exploded.filter(
        F.col("word").isin(tech_skills)
    )
    #get the high paid skill from the skillset we have
    #df_exploded.groupBy("word").count().orderBy(F.desc("count")).show(20)
    high_paid_skills = (
        df_exploded
            .groupBy("word")
            .agg(
                F.count("*").alias("job_count"),
                F.avg("Salary_Mid").alias("avg_salary")
            )
            .where(F.col("job_count") >= 30)
            .orderBy(F.desc("avg_salary"))
    )

    '''Job descriptions contain significant generic language.
    After applying standard and domain-specific stopword removal, we restricted analysis to a curated list of technical skills 
    to avoid noise from soft skills and HR terminology.This produced more meaningful salary insights aligned with market demand.'''
    # Show top highest paid skills
    high_paid_skills.show(20, False)
    return df

def data_write(spark,df,write_path):
    final_df = df.select(
        "Job ID",
        "Agency",
        "Business Title",
        "Job Category",
        "Job_Category_Exploded",
        "Salary_Mid",
        "Min_Degree",
        "Posting Date",
        "preferred_skills_clean",
        "Salary Frequency"
    )
    # Rename columns to remove spaces (required for Parquet)
    final_df = final_df.toDF(*[
        c.strip()
                             .replace(" ", "_")
                             .replace("/", "_")
                             .replace("#", "No")
        for c in final_df.columns
    ])
    #print("Rows before write:", final_df.count())
    final_df.write.mode("overwrite").parquet(write_path + "/processed_nyc_jobs.parquet")
    final_df.coalesce(1).write.mode("overwrite").option("header","true").csv(write_path + "/processed_nyc_jobs.csv")

def main(arg):
    print("Data Engineering Assessments starts here")
    spark = SparkSession.builder.appName("assessments").master("local[*]").getOrCreate()
    print("Spark Version:", spark.version)
    sc = spark.sparkContext
    print("Set the logger level to error")
    sc.setLogLevel("ERROR")

    print("Get the base location as parameter (to make the code to run in windows/VM) \n")
    read_path = arg[1] #"E:/Bigdata/assessments/data_engineering_takehome1/dataset/nyc-jobs.csv"
    write_path = arg[2] #E:/Bigdata/assessments/data_engineering_takehome1/output
    print("Here read file location is : " + read_path)
    print("Here write file location is : " + write_path)

    df = load_data(spark, read_path)
    df = data_cleaning(df)
    df = data_pre_processing(df)
    df = data_normalization(df)
    data_write(spark,df,write_path)

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        print(sys.argv)
        main(sys.argv)
    else:
        print("No enough argument to continue running this program, Please pass the input and output path")
        exit(1)