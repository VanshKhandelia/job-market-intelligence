# gold_transform.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import snowflake.connector
import pandas as pd
import os


def write_gold_to_snowflake(gold_pdf):
    """
    Writes aggregated gold dataframe (pandas) to Snowflake GOLD schema.
    """

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="JOB_INTELLIGENCE",
        schema="GOLD"
    )

    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS COMPANY_TECH_MIX (
            COMPANY_CLEAN STRING,
            CAPABILITY_DOMAIN STRING,
            JOB_COUNT INTEGER,
            TECH_SHARE FLOAT,
            TOTAL_TECH_JOBS INTEGER
        )
    """)

    # Clear old data
    cursor.execute("TRUNCATE TABLE COMPANY_TECH_MIX")

    # Insert new data
    for _, row in gold_pdf.iterrows():
        cursor.execute(
            "INSERT INTO COMPANY_TECH_MIX VALUES (%s, %s, %s, %s, %s)",
            (
                row["company_clean"],
                row["capability_domain"],
                int(row["job_count"]),
                float(row["tech_share"]),
                int(row["total_tech_jobs"])
            )
        )

    conn.commit()
    conn.close()

    print("Gold table successfully written to Snowflake.")


def main():
    spark = SparkSession.builder.appName("GoldTransform").getOrCreate()

    # Read Silver table from Databricks
    silver_df = spark.read.table("job_intelligence.silver_jobs")

    # Filter to tech capability domains only
    tech_df = silver_df.filter(
        F.col("capability_domain").isin(
            "engineering",
            "data_ai",
            "cloud_infra",
            "security",
            "networking"
        )
    )

    # Aggregate: 1 row per company per capability
    gold_df = tech_df.groupBy(
        "company_clean",
        "capability_domain"
    ).agg(
        F.count("*").alias("job_count")
    )

    # Add share per company
    window = Window.partitionBy("company_clean")

    gold_df = gold_df.withColumn(
        "tech_share",
        F.col("job_count") / F.sum("job_count").over(window)
    )

    gold_df = gold_df.withColumn(
        "total_tech_jobs",
        F.sum("job_count").over(window)
    )

    # Convert to Pandas (dataset is small)
    gold_pdf = gold_df.toPandas()

    # Write to Snowflake
    write_gold_to_snowflake(gold_pdf)


if __name__ == "__main__":
    main()