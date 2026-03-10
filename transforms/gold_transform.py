from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import snowflake.connector
import pandas as pd


SF_USER     = dbutils.widgets.get("SNOWFLAKE_USER")
SF_PASSWORD = dbutils.widgets.get("SNOWFLAKE_PASSWORD")
SF_ACCOUNT  = dbutils.widgets.get("SNOWFLAKE_ACCOUNT")


def write_gold_to_snowflake(gold_pdf):
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse="COMPUTE_WH",
        database="JOB_INTELLIGENCE",
        schema="GOLD"
    )
    cursor = conn.cursor()

    # Create table with year_month column for trend tracking
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS COMPANY_TECH_MIX (
            COMPANY_CLEAN STRING,
            CAPABILITY_DOMAIN STRING,
            YEAR_MONTH STRING,
            JOB_COUNT INTEGER,
            TOTAL_TECH_JOBS INTEGER,
            TECH_SHARE FLOAT,
            CAPABILITY_RANK INTEGER,
            DOMINANT_CAPABILITY BOOLEAN,
            TECH_INTENSITY_RANK INTEGER
        )
    """)

    # Get months being processed in this run
    months = gold_pdf["year_month"].unique().tolist()
    
    # Delete only the months we're about to insert — preserves all other historical data
    for month in months:
        cursor.execute(f"DELETE FROM COMPANY_TECH_MIX WHERE YEAR_MONTH = '{month}'")
        print(f"Cleared existing data for {month}")

    # Insert row by row
    for _, row in gold_pdf.iterrows():
        cursor.execute(
            "INSERT INTO COMPANY_TECH_MIX VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                row["company_clean"],
                row["capability_domain"],
                row["year_month"],
                int(row["job_count"]),
                int(row["total_tech_jobs"]),
                float(row["tech_share"]),
                int(row["capability_rank"]),
                bool(row["dominant_capability"]),
                int(row["tech_intensity_rank"])
            )
        )

    conn.commit()
    conn.close()
    print(f"✅ Gold table updated for months: {months}")


def main():
    spark = SparkSession.builder.appName("GoldTransform").getOrCreate()

    silver_df = spark.read.table("job_intelligence.silver_jobs")

    # Filter tech domains only
    tech_df = silver_df.filter(
        F.col("capability_domain").isin(
            "engineering", "data_ai", "cloud_infra", "security", "networking"
        )
    )

    # Aggregate by company + capability + month — this is what enables trends
    gold_df = tech_df.groupBy(
        "company_clean",
        "capability_domain",
        "year_month"
    ).agg(
        F.count("*").alias("job_count")
    )

    # Window per company per month
    company_month_window = Window.partitionBy("company_clean", "year_month")
    rank_window = Window.partitionBy("company_clean", "year_month").orderBy(F.desc("job_count"))

    # Total tech jobs per company per month
    gold_df = gold_df.withColumn(
        "total_tech_jobs",
        F.sum("job_count").over(company_month_window)
    )

    # Tech share — what % of this company's tech jobs are in this domain this month
    gold_df = gold_df.withColumn(
        "tech_share",
        F.round(F.col("job_count") / F.col("total_tech_jobs"), 4)
    )

    # Rank capabilities within company within month
    gold_df = gold_df.withColumn(
        "capability_rank",
        F.rank().over(rank_window)
    )

    gold_df = gold_df.withColumn(
        "dominant_capability",
        F.when(F.col("capability_rank") == 1, True).otherwise(False)
    )

    # Rank companies by total tech hiring per month
    company_rank_window = Window.partitionBy("year_month").orderBy(F.desc("total_tech_jobs"))
    gold_df = gold_df.withColumn(
        "tech_intensity_rank",
        F.dense_rank().over(company_rank_window)
    )

    gold_pdf = gold_df.toPandas()

    print(f"Gold records to write: {len(gold_pdf)}")
    print(f"Months covered: {sorted(gold_pdf['year_month'].unique())}")
    print(f"Companies covered: {gold_pdf['company_clean'].nunique()}")

    write_gold_to_snowflake(gold_pdf)


main()