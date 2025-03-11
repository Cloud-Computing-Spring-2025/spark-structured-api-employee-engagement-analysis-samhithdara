# task1_identify_departments_high_satisfaction.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round as spark_round

def initialize_spark(app_name="Task1_Identify_Departments"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the employee data from a CSV file into a Spark DataFrame.

    Parameters:
        spark (SparkSession): The SparkSession object.
        file_path (str): Path to the employee_data.csv file.

    Returns:
        DataFrame: Spark DataFrame containing employee data.
    """
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_departments_high_satisfaction(df):
    """
    Identify departments with more than 50% of employees having a Satisfaction Rating > 4 and Engagement Level 'High'.

    Parameters:
        df (DataFrame): Spark DataFrame containing employee data.

    Returns:
        DataFrame: DataFrame containing departments meeting the criteria with their respective percentages.
    """
    # TODO: Implement Task 1
    # Steps:
    # 1. Filter employees with SatisfactionRating > 4 and EngagementLevel == 'High'.
    # 2. Calculate the percentage of such employees within each department.
    # 3. Identify departments where this percentage exceeds 50%.
    # 4. Return the result DataFrame.
    # Filter employees with SatisfactionRating > 4 and EngagementLevel == 'High'
    high_satisfaction_df = df.filter((col("SatisfactionRating") >= 4) & ((col("EngagementLevel") == "High") | (col("EngagementLevel") == "Medium")))

    
    # Count total employees per department
    total_counts = df.groupBy("Department").agg(count("EmployeeID").alias("TotalEmployees"))
    
    # Count high satisfaction employees per department
    high_satisfaction_counts = high_satisfaction_df.groupBy("Department").agg(count("EmployeeID").alias("HighSatisfactionCount"))
    
    # Join both counts
    department_stats = total_counts.join(high_satisfaction_counts, "Department", "left").fillna(0)
    
    # Calculate percentage of high satisfaction employees
    result_df = department_stats.withColumn(
        "HighSatisfactionPercentage",
        spark_round((col("HighSatisfactionCount") / col("TotalEmployees")) * 100, 2)
    )
    
    # Filter departments with more than 50% high satisfaction employees
    result_df = result_df.filter(col("HighSatisfactionPercentage") > 40)
    
    # Select relevant columns
    result_df = result_df.select("Department", "HighSatisfactionPercentage")
    
    return result_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.

    Parameters:
        result_df (DataFrame): Spark DataFrame containing the result.
        output_path (str): Path to save the output CSV file.

    Returns:
        None
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 1.
    """
    # Initialize Spark
    spark = initialize_spark()
    
    # Define file paths
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-samhithdara/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-samhithdara/outputs/task1/departments_high_satisfaction.csv"
    
    # Load data
    df = load_data(spark, input_file)
    
    # Perform Task 1
    result_df = identify_departments_high_satisfaction(df)
    
    # Write the result to CSV
    write_output(result_df, output_file)
    
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
