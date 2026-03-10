# silver_transform.py
# Run this in Databricks — fill in widgets at the top before running

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import snowflake.connector
import pandas as pd

# ── Databricks widgets for credentials ──────────────────────────────

SF_USER     = dbutils.widgets.get("SNOWFLAKE_USER")
SF_PASSWORD = dbutils.widgets.get("SNOWFLAKE_PASSWORD")
SF_ACCOUNT  = dbutils.widgets.get("SNOWFLAKE_ACCOUNT")

# ── Snowflake connection helper ──────────────────────────────────────
def get_conn(schema):
    return snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse="COMPUTE_WH",
        database="JOB_INTELLIGENCE",
        schema=schema
    )

# ── Load only NEW Bronze records ─────────────────────────────────────
def load_new_bronze():
    conn = get_conn("BRONZE")
    # Check max extracted_at already in Databricks Silver table
    try:
        max_ts = spark.sql("SELECT MAX(extracted_at) FROM job_intelligence.silver_jobs") \
                      .collect()[0][0]
        if max_ts is None:
            max_ts = "2000-01-01"
        else:
            max_ts = str(max_ts)
        print(f"Loading Bronze records newer than: {max_ts}")
    except:
        # Silver table doesn't exist yet — first run, load everything
        max_ts = "2000-01-01"
        print("First run — loading all Bronze records")

    query = f"SELECT * FROM RAW_JOB_POSTINGS WHERE extracted_at > '{max_ts}'"
    pdf = pd.read_sql(query, conn)
    conn.close()
    print(f"New Bronze records to process: {len(pdf)}")
    return pdf

# ── Main transformation ──────────────────────────────────────────────
def main():
    spark = SparkSession.builder.appName("SilverTransform").getOrCreate()

    bronze_pdf = load_new_bronze()

    if bronze_pdf.empty:
        print("✅ No new records to process — Silver is up to date")
        return

    bronze_df = spark.createDataFrame(bronze_pdf)

    # Lowercase column names
    silver_df = bronze_df.toDF(*[c.lower() for c in bronze_df.columns])

    # ── Basic cleaning ───────────────────────────────────────────────
    silver_df = (
        silver_df
        .dropDuplicates(["job_id"])
        .filter(F.col("job_title").isNotNull())
        .filter(F.length(F.col("job_title")) > 3)  # remove garbage titles
        .withColumn("title_clean", F.lower(F.col("job_title")))
        .withColumn("company_clean",
            F.trim(F.regexp_replace(F.lower(F.col("company_name_searched")),
                r"\s+(inc|ltd|llc|corp|corporation)\.?$", ""))
        )
        .withColumn("city",    F.trim(F.split(F.col("location"), ",")[0]))
        .withColumn("year",    F.year("created"))
        .withColumn("month",   F.month("created"))
        .withColumn("year_month", F.date_format("created", "yyyy-MM"))
        .withColumn("processed_at", F.current_timestamp())
    )

    # ── Seniority extraction from title ─────────────────────────────
    silver_df = silver_df.withColumn(
        "seniority",
        F.when(F.col("title_clean").rlike("vp|vice president|chief|cto|cio"), "Executive")
        .when(F.col("title_clean").rlike("director|head of"), "Director")
        .when(F.col("title_clean").rlike("senior|sr\\.|lead|principal|staff"), "Senior")
        .when(F.col("title_clean").rlike("junior|jr\\.|associate|entry"), "Junior")
        .when(F.col("title_clean").rlike("manager"), "Manager")
        .otherwise("Mid")
    )

    # ── Remote flag ──────────────────────────────────────────────────
    silver_df = silver_df.withColumn(
        "is_remote",
        F.when(
            F.lower(F.col("job_title")).contains("remote") |
            F.lower(F.col("location")).contains("remote"),
            True
        ).otherwise(False)
    )

    # ── Capability domain ────────────────────────────────────────────
    # Order matters — most specific patterns first
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


    # ── Write to Silver Delta table (append — preserve history) ──────
    silver_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true")\
        .saveAsTable("job_intelligence.silver_jobs")

    print(f"✅ Silver table updated with new records")
    print(f"Capability breakdown:")
    silver_df.groupBy("capability_domain").count().orderBy(F.desc("count")).show()

    

# ── Run ──────────────────────────────────────────────────────────────
main()