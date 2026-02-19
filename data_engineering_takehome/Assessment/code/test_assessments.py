from pyspark.sql import SparkSession
from assessments import data_cleaning, data_pre_processing, data_normalization

# Create Spark session for testing
spark = SparkSession.builder.master("local[*]").appName("unit-test").getOrCreate()

def create_test_df():
    data = [
        (
            "101", "Agency A", "Data Engineer", "IT, Engineering", "2",
            70000.0, 90000.0, "Annual",
            "Bachelor degree in Computer Science required",
            "2023-01-10T00:00:00.000", "", "2023-06-01T00:00:00.000",
            "Python SQL Spark", "2024-06-01T00:00:00.000"
        ),
        (
            "102", "Agency B", "Project Manager", "Management", "1",
            85000.0, 110000.0, "Annual",
            "Master degree preferred",
            "2022-05-15T00:00:00.000", "", "2022-10-01T00:00:00.000",
            "Leadership Communication", "2023-10-01T00:00:00.000"
        ),
        (
            "103", "Agency A", "Software Developer", "IT", "3",
            60000.0, 80000.0, "Annual",
            "Bachelor degree required",
            "2021-03-20T00:00:00.000", "", "2021-08-01T00:00:00.000",
            "Java Python Git", "2022-08-01T00:00:00.000"
        ),
        (
            "104", "Agency C", "Data Scientist", "IT, Analytics", "1",
            95000.0, 130000.0, "Annual",
            "PhD degree in Statistics",
            "2023-07-01T00:00:00.000", "", "2023-09-01T00:00:00.000",
            "Python MachineLearning SQL", "2024-09-01T00:00:00.000"
        )
    ]

    columns = [
        "Job ID", "Agency", "Business Title", "Job Category","# Of Positions",
        "Salary Range From", "Salary Range To", "Salary Frequency",
        "Minimum Qual Requirements",
        "Posting Date","Post Until", "Posting Updated","Preferred Skills","Process Date"
    ]
    df = spark.createDataFrame(data, columns)
    return df

def test_salary_mid_creation():
    df = create_test_df()
    df = data_cleaning(df)
    df = data_pre_processing(df)
    df = data_normalization(df)

    assert "Salary_Mid" in df.columns, "Salary_Mid column not created"
    print("✔ Salary_Mid column test passed")

def test_salary_mid_value():
    df = create_test_df()
    df = data_cleaning(df)
    df = data_pre_processing(df)
    df = data_normalization(df)

    result = df.select("Salary_Mid").collect()[0][0]

    assert abs(result - 80000.0) < 0.01, "Salary_Mid calculation incorrect"
    print("✔ Salary_Mid value test passed")

if __name__ == "__main__":
    test_salary_mid_creation()
    test_salary_mid_value()
    print("\nAll tests passed successfully!")
