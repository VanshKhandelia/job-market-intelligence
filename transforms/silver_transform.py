# silver_transform.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import snowflake.connector
import pandas as pd
import os


def load_bronze_from_snowflake():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="JOB_INTELLIGENCE",
        schema="BRONZE"
    )

    query = "SELECT * FROM RAW_JOB_POSTINGS"
    pdf = pd.read_sql(query, conn)
    conn.close()
    return pdf


def main():
    spark = SparkSession.builder.appName("SilverTransform").getOrCreate()

    # Load Bronze from Snowflake
    bronze_pdf = load_bronze_from_snowflake()

    bronze_df = spark.createDataFrame(bronze_pdf)

    # Lowercase columns
    silver_df = bronze_df.toDF(*[c.lower() for c in bronze_df.columns])

    silver_df = silver_df.dropDuplicates(["job_id"])

    silver_df = (
        silver_df
        .filter(F.col("description").isNotNull())
        .withColumn("title_clean", F.lower("job_title"))
        .withColumn("year", F.year("created"))
        .withColumn("month", F.month("created"))
        .withColumn("year_month", F.date_format("created", "yyyy-MM"))
        .withColumn("company_clean",
            F.regexp_replace(F.lower("company_name_searched"),
                             " inc| ltd| llc| corp| corporation", "")
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    # Capability classification (simplified for clarity)
    silver_df = silver_df.withColumn(
        "capability_domain",
        F.when(F.col("title_clean").rlike("machine learning|ml engineer|ai|deep learning"), "data_ai")
        .when(F.col("title_clean").rlike("data engineer|etl|analytics|business intelligence|data"), "data_ai")
        .when(F.col("title_clean").rlike("cloud|devops|site reliability|sre|infrastructure|platform"), "cloud_infra")
        .when(F.col("title_clean").rlike("security|cyber|soc|threat|vulnerability|iam|pentests"), "security")
        .when(F.col("title_clean").rlike("network|telecom|5g|csp|routing|switching|communications"), "networking")
        .when(F.col("title_clean").rlike("sales|presales|solutions architect|account|product manager|product owner| manager|business development|ventes"), "gtm")
        .when(F.col("title_clean").rlike("engineer|developer|software|programmer|architect|platform|qa|test|software engineer|backend|frontend|developer|platform engineer|technical|implementation|UI|deployment|migration|IT"), "engineering")
        .when(F.col("title_clean").rlike("support|service delivery|field service|technician|application,support"),"support_services")
        .otherwise("other")
    )

    # Write Silver to Delta
    silver_df.write.format("delta").mode("overwrite").saveAsTable(
        "job_intelligence.silver_jobs"
    )

    print("Silver table created.")


if __name__ == "__main__":
    main()