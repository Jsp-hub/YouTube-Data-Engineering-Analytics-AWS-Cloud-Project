"""
Lambda: Data Quality Checks
────────────────────────────
Called by Step Functions after the Silver layer is built.
Validates data quality before allowing the Gold aggregation to proceed.

Checks performed:
  1. Row count — is there enough data?
  2. Null percentage — are critical columns populated?
  3. Schema validation — do expected columns exist?
  4. Value range checks — are numeric values reasonable?
  5. Freshness — is the data recent enough?

Environment Variables:
    S3_BUCKET_SILVER        — Silver bucket to check
    SNS_ALERT_TOPIC_ARN     — SNS for alerts
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
import time
import pandas as pd


logger = logging.getLogger()
logger.setLevel(logging.INFO)



sns_client = boto3.client("sns")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
ATHENA_OUTPUT = os.environ.get(
    "ATHENA_OUTPUT_BUCKET",
    "s3://query-result-bucket-using-athena/"
)

# ── Thresholds ───────────────────────────────────────────────────────────────
MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
MAX_VIEWS = 50_000_000_000  # 50B — sanity check for view counts
FRESHNESS_HOURS = 48  # Data should be no older than this


CRITICAL_COLUMNS = {
    "raw_statistic_data_inpq": ["video_id", "title", "channel_title", "views", "region"],
    "raw_ref_data_inpq": ["id", "region"],
}

#---------------using boto3 instead awswrangler--------------

def athena_query_to_dataframe(query, database, output_location):
    """
    Execute Athena query using boto3 and return Pandas DataFrame.
    """

    athena_client = boto3.client("athena")

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": database
        },
        ResultConfiguration={
            "OutputLocation": output_location
        }
    )

    query_execution_id = response["QueryExecutionId"]

    logger.info(f"Athena Query ID: {query_execution_id}")

    # Wait until query completes
    while True:
        execution = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status = execution["QueryExecution"]["Status"]["State"]

        logger.info(f"Athena status: {status}")

        if status == "SUCCEEDED":
            break

        if status in ["FAILED", "CANCELLED"]:
            reason = execution["QueryExecution"]["Status"].get(
                "StateChangeReason",
                "Unknown reason"
            )
            raise Exception(
                f"Athena query failed: {reason}"
            )

        time.sleep(2)


    # Get results
    paginator = athena_client.get_paginator(
        "get_query_results"
    )

    rows = []
    columns = None

    for page in paginator.paginate(
        QueryExecutionId=query_execution_id
    ):

        page_rows = page["ResultSet"]["Rows"]

        # First row is header
        if columns is None:
            columns = [
                col.get("VarCharValue")
                for col in page_rows[0]["Data"]
            ]
            page_rows = page_rows[1:]

        for row in page_rows:
            values = [
                cell.get("VarCharValue")
                for cell in row["Data"]
            ]

            rows.append(values)


    df = pd.DataFrame(
        rows,
        columns=columns
    )
    # Convert numeric columns
    numeric_columns = [
        "views",
        "likes",
        "dislikes",
        "comment_count"
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )
    return df

def check_row_count(df: pd.DataFrame, table_name: str) -> dict:
    """Check that table has minimum number of rows."""
    count = len(df)
    passed = count >= MIN_ROW_COUNT
    return {
        "check": "row_count",
        "table": table_name,
        "value": count,
        "threshold": MIN_ROW_COUNT,
        "passed": passed,
        "message": f"Row count: {count} (min: {MIN_ROW_COUNT})",
    }


def check_null_percentage(df: pd.DataFrame, table_name: str) -> list:
    """Check null percentages for critical columns."""
    results = []
    cols = CRITICAL_COLUMNS.get(table_name, [])

    for col in cols:
        if col not in df.columns:
            results.append({
                "check": "null_pct",
                "table": table_name,
                "column": col,
                "passed": False,
                "message": f"Column '{col}' missing from table",
            })
            continue

        null_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 0
        passed = null_pct <= MAX_NULL_PCT
        results.append({
            "check": "null_pct",
            "table": table_name,
            "column": col,
            "value": round(null_pct, 2),
            "threshold": MAX_NULL_PCT,
            "passed": passed,
            "message": f"{col} null%: {null_pct:.2f}% (max: {MAX_NULL_PCT}%)",
        })

    return results


def check_schema(df: pd.DataFrame, table_name: str) -> dict:
    """Check that expected columns exist."""
    expected = set(CRITICAL_COLUMNS.get(table_name, []))
    actual = set(df.columns)
    missing = expected - actual
    passed = len(missing) == 0
    return {
        "check": "schema",
        "table": table_name,
        "missing_columns": list(missing),
        "passed": passed,
        "message": f"Missing columns: {missing}" if missing else "All expected columns present",
    }


def check_value_ranges(df: pd.DataFrame, table_name: str) -> list:
    """Check that numeric values are within reasonable ranges."""
    results = []

    if table_name != "raw_statistic_data_inpq":
        return results

    if "views" in df.columns:
        negative = (df["views"] < 0).sum()
        extreme = (df["views"] > MAX_VIEWS).sum()
        passed = negative == 0 and extreme == 0
        results.append({
            "check": "value_range",
            "table": table_name,
            "column": "views",
            "negative_count": int(negative),
            "extreme_count": int(extreme),
            "passed": passed,
            "message": f"Views: {negative} negative, {extreme} extreme (>{MAX_VIEWS})",
        })

    return results

def check_freshness(df: pd.DataFrame, table_name: str) -> dict:
    """Check that data includes recent records."""

    if "_processed_at" not in df.columns and "_ingestion_timestamp" not in df.columns:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": "No timestamp column found — skipping freshness check",
        }

    ts_col = "_processed_at" if "_processed_at" in df.columns else "_ingestion_timestamp"

    try:
        logger.info(f"Freshness check using column: {ts_col}")

        timestamps = df[ts_col].astype(str).tolist()

        logger.info(f"Timestamp sample: {timestamps[:5]}")

        # Parse timestamps using pure Python
        parsed_times = []

        for ts in timestamps:
            try:
                # Handle ISO format timestamps ending with Z
                parsed = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )

                # Handle timestamps without timezone
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)

                parsed_times.append(parsed)

            except Exception:
                continue

        logger.info(f"Successfully parsed timestamps: {len(parsed_times)}")

        if not parsed_times:
            return {
                "check": "freshness",
                "table": table_name,
                "passed": False,
                "message": "No valid timestamps found",
            }

        latest = max(parsed_times)

        cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)

        passed = latest >= cutoff

        logger.info(f"Latest timestamp: {latest}")
        logger.info(f"Cutoff timestamp: {cutoff}")
        logger.info(f"Freshness passed: {passed}")

        return {
            "check": "freshness",
            "table": table_name,
            "latest_record": str(latest),
            "cutoff": str(cutoff),
            "passed": bool(passed),
            "message": f"Latest: {latest}, Cutoff: {cutoff}",
        }

    except Exception as e:
        logger.exception("Freshness check failed")

        return {
            "check": "freshness",
            "table": table_name,
            "passed": False,
            "message": str(e),
        }
# def check_freshness(df: pd.DataFrame, table_name: str) -> dict:
#     """Check that data includes recent records."""

#     if "_processed_at" not in df.columns and "_ingestion_timestamp" not in df.columns:
#         return {
#             "check": "freshness",
#             "table": table_name,
#             "passed": True,
#             "message": "No timestamp column found — skipping freshness check",
#         }

#     ts_col = "_processed_at" if "_processed_at" in df.columns else "_ingestion_timestamp"

#     try:
#         logger.info(f"Freshness check using column: {ts_col}")

#         timestamps = df[ts_col].astype(str).tolist()

#         logger.info(f"Timestamp sample: {timestamps[:5]}")

#         parsed_times = []

#         for ts in timestamps:
#             try:
#                 parsed_times.append(
#                     datetime.strptime(
#                         ts,
#                         "%Y-%m-%d %H:%M:%S.%f"
#                     )
#                 )
#             except ValueError:
#                 continue

#         if not parsed_times:
#             return {
#                 "check": "freshness",
#                 "table": table_name,
#                 "passed": False,
#                 "message": "No valid timestamps found",
#             }

#         latest = max(parsed_times)

#         latest = latest.replace(tzinfo=timezone.utc)

#         cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)

#         passed = latest >= cutoff

#         return {
#             "check": "freshness",
#             "table": table_name,
#             "latest_record": str(latest),
#             "cutoff": str(cutoff),
#             "passed": passed,
#             "message": f"Latest: {latest}, Cutoff: {cutoff}",
#         }

#     except Exception as e:
#         logger.exception("Freshness check failed")

#         return {
#             "check": "freshness",
#             "table": table_name,
#             "passed": False,
#             "message": str(e),
#         }

def lambda_handler(event, context):
    """
    Run data quality checks on Silver layer tables.

    Expected event:
    {
        "layer": "silver",
        "database": "yt-data-pipeline-silver-gluedb",
        "tables": ["raw_statistic_data_inpq", "raw_ref_data_inpq"]
    }
    """
    database = event.get("database", "yt-data-pipeline-silver-gluedb")
    tables = event.get("tables", ["raw_ref_data_inpq"])             #Here only one table is validated; so if you run this function individually(no step function) then add the second table also. Because I am using step function to orchestration; I have provided both of the tables in step function: checkout step_functions/pipeline_orchestration.json - line: 119

    all_results = []
    overall_passed = True

    for table_name in tables:
        logger.info(f"Running DQ checks on {database}.{table_name}...")

        try:
            # Read a sample of the data (limit for cost/speed)
            query = f'SELECT * FROM "{table_name}" LIMIT 10'

            logger.info("Before Athena boto3 query")

            df = athena_query_to_dataframe(
                query=query,
                database=database,
                output_location=ATHENA_OUTPUT
            )

            logger.info("After Athena boto3 query")


             # ===== DEBUG LOGS =====
            logger.info(f"Successfully read table: {table_name}")
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"Columns: {df.columns.tolist()}")
            logger.info(f"First 5 rows:\n{df.head().to_string()}")

        except Exception as e:
            logger.error(f"Could not read {table_name}: {e}")
            all_results.append({
                "check": "read_table",
                "table": table_name,
                "passed": False,
                "message": str(e),
            })
            overall_passed = False
            continue

        # Run all checks
        checks = []
        logger.info("Starting row count check")
        checks.append(check_row_count(df, table_name))

        logger.info("Starting null check")
        checks.extend(check_null_percentage(df, table_name))

        logger.info("Starting schema check")
        checks.append(check_schema(df, table_name))

        logger.info("Starting value range check")
        checks.extend(check_value_ranges(df, table_name))

        logger.info("Starting freshness check")
        checks.append(check_freshness(df, table_name))

        logger.info("All checks completed")

        for check in checks:
            logger.info(f"  {check['check']}: {'PASS' if check['passed'] else 'FAIL'} — {check['message']}")
            if not check["passed"]:
                overall_passed = False

        all_results.extend(checks)

    # Summary
    passed_count = sum(1 for r in all_results if r["passed"])
    total_count = len(all_results)
    logger.info(f"DQ Summary: {passed_count}/{total_count} checks passed. Overall: {'PASS' if overall_passed else 'FAIL'}")

    if not overall_passed and SNS_TOPIC:
        failed = [r for r in all_results if not r["passed"]]
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject="[YT Pipeline] Data quality checks FAILED",
            Message=json.dumps(failed, indent=2, default=str),
        )

    return {
        "quality_passed": bool(overall_passed),
        "checks_passed": int(passed_count),
        "checks_total": int(total_count),
        "details": json.loads(json.dumps(all_results, default=str)),
    }


# """
# Lambda: Data Quality Checks
# ────────────────────────────
# Called by Step Functions after the Silver layer is built.
# Validates data quality before allowing the Gold aggregation to proceed.

# Checks performed:
#   1. Row count — is there enough data?
#   2. Null percentage — are critical columns populated?
#   3. Schema validation — do expected columns exist?
#   4. Value range checks — are numeric values reasonable?
#   5. Freshness — is the data recent enough?

# Environment Variables:
#     S3_BUCKET_SILVER        — Silver bucket to check
#     SNS_ALERT_TOPIC_ARN     — SNS for alerts
# """

# import os
# import json
# import logging
# from datetime import datetime, timezone, timedelta

# import boto3
# import awswrangler as wr
# import pandas as pd

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# sns_client = boto3.client("sns")
# SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
# ATHENA_OUTPUT = os.environ.get(
#     "ATHENA_OUTPUT_BUCKET",
#     "s3://query-result-bucket-using-athena/"
# )

# # ── Thresholds ───────────────────────────────────────────────────────────────
# MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
# MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
# MAX_VIEWS = 50_000_000_000  # 50B — sanity check for view counts
# FRESHNESS_HOURS = 48  # Data should be no older than this


# CRITICAL_COLUMNS = {
#     "raw_statistic_data_inpq": ["video_id", "title", "channel_title", "views", "region"],
#     "raw_ref_data_inpq": ["id", "region"],
# }


# def check_row_count(df: pd.DataFrame, table_name: str) -> dict:
#     """Check that table has minimum number of rows."""
#     count = len(df)
#     passed = count >= MIN_ROW_COUNT
#     return {
#         "check": "row_count",
#         "table": table_name,
#         "value": count,
#         "threshold": MIN_ROW_COUNT,
#         "passed": passed,
#         "message": f"Row count: {count} (min: {MIN_ROW_COUNT})",
#     }


# def check_null_percentage(df: pd.DataFrame, table_name: str) -> list:
#     """Check null percentages for critical columns."""
#     results = []
#     cols = CRITICAL_COLUMNS.get(table_name, [])

#     for col in cols:
#         if col not in df.columns:
#             results.append({
#                 "check": "null_pct",
#                 "table": table_name,
#                 "column": col,
#                 "passed": False,
#                 "message": f"Column '{col}' missing from table",
#             })
#             continue

#         null_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 0
#         passed = null_pct <= MAX_NULL_PCT
#         results.append({
#             "check": "null_pct",
#             "table": table_name,
#             "column": col,
#             "value": round(null_pct, 2),
#             "threshold": MAX_NULL_PCT,
#             "passed": passed,
#             "message": f"{col} null%: {null_pct:.2f}% (max: {MAX_NULL_PCT}%)",
#         })

#     return results


# def check_schema(df: pd.DataFrame, table_name: str) -> dict:
#     """Check that expected columns exist."""
#     expected = set(CRITICAL_COLUMNS.get(table_name, []))
#     actual = set(df.columns)
#     missing = expected - actual
#     passed = len(missing) == 0
#     return {
#         "check": "schema",
#         "table": table_name,
#         "missing_columns": list(missing),
#         "passed": passed,
#         "message": f"Missing columns: {missing}" if missing else "All expected columns present",
#     }


# def check_value_ranges(df: pd.DataFrame, table_name: str) -> list:
#     """Check that numeric values are within reasonable ranges."""
#     results = []

#     if table_name != "raw_statistic_data_inpq":
#         return results

#     if "views" in df.columns:
#         negative = (df["views"] < 0).sum()
#         extreme = (df["views"] > MAX_VIEWS).sum()
#         passed = negative == 0 and extreme == 0
#         results.append({
#             "check": "value_range",
#             "table": table_name,
#             "column": "views",
#             "negative_count": int(negative),
#             "extreme_count": int(extreme),
#             "passed": passed,
#             "message": f"Views: {negative} negative, {extreme} extreme (>{MAX_VIEWS})",
#         })

#     return results


# def check_freshness(df: pd.DataFrame, table_name: str) -> dict:
#     """Check that data includes recent records."""
#     if "_processed_at" not in df.columns and "_ingestion_timestamp" not in df.columns:
#         return {
#             "check": "freshness",
#             "table": table_name,
#             "passed": True,
#             "message": "No timestamp column found — skipping freshness check (backfill data)",
#         }

#     ts_col = "_processed_at" if "_processed_at" in df.columns else "_ingestion_timestamp"
#     try:
#         latest = pd.to_datetime(df[ts_col]).max()
#         cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)
#         # Handle timezone-naive timestamps
#         if latest.tzinfo is None:
#             latest = latest.replace(tzinfo=timezone.utc)
#         passed = latest >= cutoff
#         return {
#             "check": "freshness",
#             "table": table_name,
#             "latest_record": str(latest),
#             "cutoff": str(cutoff),
#             "passed": passed,
#             "message": f"Latest: {latest}, Cutoff: {cutoff}",
#         }
#     except Exception as e:
#         return {
#             "check": "freshness",
#             "table": table_name,
#             "passed": True,
#             "message": f"Could not parse timestamps: {e} — skipping",
#         }


# def lambda_handler(event, context):
#     """
#     Run data quality checks on Silver layer tables.

#     Expected event:
#     {
#         "layer": "silver",
#         "database": "yt-data-pipeline-silver-gluedb",
#         "tables": ["raw_statistic_data_inpq", "raw_ref_data_inpq"]
#     }
#     """
#     database = event.get("database", "yt-data-pipeline-silver-gluedb")
#     tables = event.get("tables", ["raw_statistic_data_inpq"])             #Here only one table is validated; so if you run this function individually(no step function) then add the second table also. Because I am using step function to orchestration; I have provided both of the tables in step function: checkout step_functions/pipeline_orchestration.json - line: 119

#     all_results = []
#     overall_passed = True

#     for table_name in tables:
#         logger.info(f"Running DQ checks on {database}.{table_name}...")

#         try:
#             # Read a sample of the data (limit for cost/speed)
#             query = f'SELECT * FROM "{table_name}" LIMIT 10000'
#             df = wr.athena.read_sql_query(
#                 sql=query,
#                 database=database,
#                 s3_output=ATHENA_OUTPUT,
#                 ctas_approach=False,
#             )
#         except Exception as e:
#             logger.error(f"Could not read {table_name}: {e}")
#             all_results.append({
#                 "check": "read_table",
#                 "table": table_name,
#                 "passed": False,
#                 "message": str(e),
#             })
#             overall_passed = False
#             continue

#         # Run all checks
#         checks = []
#         checks.append(check_row_count(df, table_name))
#         checks.extend(check_null_percentage(df, table_name))
#         checks.append(check_schema(df, table_name))
#         checks.extend(check_value_ranges(df, table_name))
#         checks.append(check_freshness(df, table_name))

#         for check in checks:
#             logger.info(f"  {check['check']}: {'PASS' if check['passed'] else 'FAIL'} — {check['message']}")
#             if not check["passed"]:
#                 overall_passed = False

#         all_results.extend(checks)

#     # Summary
#     passed_count = sum(1 for r in all_results if r["passed"])
#     total_count = len(all_results)
#     logger.info(f"DQ Summary: {passed_count}/{total_count} checks passed. Overall: {'PASS' if overall_passed else 'FAIL'}")

#     if not overall_passed and SNS_TOPIC:
#         failed = [r for r in all_results if not r["passed"]]
#         sns_client.publish(
#             TopicArn=SNS_TOPIC,
#             Subject="[YT Pipeline] Data quality checks FAILED",
#             Message=json.dumps(failed, indent=2, default=str),
#         )

#     return {
#         "quality_passed": bool(overall_passed),
#         "checks_passed": int(passed_count),
#         "checks_total": int(total_count),
#         "details": json.loads(json.dumps(all_results, default=str)),
#     }