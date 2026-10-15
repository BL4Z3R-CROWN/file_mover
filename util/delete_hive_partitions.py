import argparse
import concurrent.futures
import logging
import subprocess
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s:: %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S'
)

logger = logging.getLogger(__name__)


def get_matching_hive_partitions(spark, table_name, tbl_dt_values):
    """
    Use SHOW PARTITIONS (a pure Hive Metastore metadata operation) instead of
    SELECT COUNT(*), which triggers HadoopRDD.getInputFormat() and fails with
    java.lang.InstantiationException if InputFormat is unset/abstract on any partition.
    """
    target_set = {str(dt) for dt in tbl_dt_values}
    partitions_df = spark.sql(f"SHOW PARTITIONS {table_name}")
    all_partitions = [row[0] for row in partitions_df.collect()]

    matched_partitions = []
    for part_spec in all_partitions:
        # part_spec looks like "tbl_dt=20260629" (or "tbl_dt=20260629/...")
        for token in part_spec.split("/"):
            if token.startswith("tbl_dt="):
                val = token.split("=", 1)[1].strip("'\"")
                if val in target_set:
                    matched_partitions.append(part_spec)
                break

    return sorted(matched_partitions)


def inspect_table_metadata(spark, table_name):
    """
    Log key metadata from DESCRIBE FORMATTED so we can see Location, InputFormat,
    Table Type, and whether the table is MANAGED, EXTERNAL, or ICEBERG.
    """
    try:
        desc_rows = spark.sql(f"DESCRIBE FORMATTED {table_name}").collect()
        interesting_keys = {
            "Type",
            "Provider",
            "Location",
            "Serde Library",
            "InputFormat",
            "OutputFormat",
            "Table Properties",
        }
        meta_lines = []
        for row in desc_rows:
            col_name = (row[0] or "").strip().rstrip(":")
            data_type = (row[1] or "").strip()
            if col_name in interesting_keys:
                meta_lines.append(f"  {col_name}: {data_type}")
        if meta_lines:
            logger.info(
                "Table metadata for %s:\n%s",
                table_name,
                "\n".join(meta_lines)
            )
    except Exception as exc:
        logger.warning(f"Could not inspect metadata for {table_name}: {exc}")


def delete_hive_tbl_dt_partitions(
    table_name,
    tbl_dt_values,
    dry_run=False,
    batch_size=100
):
    spark = SparkSession \
        .builder \
        .appName(f"drop_hive_partitions:{table_name}") \
        .config("spark.driver.maxResultSize", "256g") \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .config("spark.sql.parquet.writeLegacyFormat", "false") \
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://10.1.154.3:9000") \
        .config("hive.metastore.uris", "thrift://10.1.154.38:9083") \
        .config("spark.sql.warehouse.dir", "hdfs://10.1.154.3:9000/warehouse/hive") \
        .config("spark.sql.shuffle.partitions", 200) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.parquet.enableVectorizedWriter", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('OFF')

    if not tbl_dt_values:
        raise ValueError("tbl_dt_values cannot be empty.")

    logger.info(
        "%sPreparing to drop tbl_dt partitions %s from Hive table %s",
        "[DRY RUN] " if dry_run else "",
        tbl_dt_values,
        table_name
    )

    # Inspect table metadata (Location, InputFormat, Type, etc.)
    inspect_table_metadata(spark, table_name)

    # 1. Drop partitions from the Hive Metastore in batches (tbl_dt is INT).
    for i in range(0, len(tbl_dt_values), batch_size):
        batch = tbl_dt_values[i:i + batch_size]
        partition_specs = ",\n            ".join(
            f"PARTITION (tbl_dt={int(dt)})" for dt in batch
        )
        drop_partition_sql = f"""
        ALTER TABLE {table_name}
        DROP IF EXISTS
            {partition_specs}
        PURGE
        """

        if dry_run:
            logger.info(f"[DRY RUN] Would execute Hive SQL:\n{drop_partition_sql}")
        else:
            logger.info(f"Running Hive drop partitions:\n{drop_partition_sql}")
            spark.sql(drop_partition_sql)

    # 2. Check / Validate partitions in Hive Metastore via SHOW PARTITIONS.
    if dry_run:
        logger.info("[DRY RUN] Checking existing partitions in Hive Metastore via SHOW PARTITIONS...")
        matched = get_matching_hive_partitions(spark, table_name, tbl_dt_values)
        logger.info(
            "[DRY RUN] Matching partitions currently registered in Hive Metastore (%d):\n%s",
            len(matched),
            "\n".join(matched) if matched else "None"
        )
        return

    logger.info("Validating dropped partitions via SHOW PARTITIONS...")
    remaining_partitions = get_matching_hive_partitions(spark, table_name, tbl_dt_values)
    remaining_count = len(remaining_partitions)
    logger.info(f"remaining_count after dropping partitions: {remaining_count}")

    if remaining_count > 0:
        logger.error(
            "Partition drop validation failed. %d tbl_dt partitions still exist in %s:\n%s",
            remaining_count,
            table_name,
            "\n".join(remaining_partitions)
        )
        raise RuntimeError(
            f"Partition drop validation failed for tbl_dt values: {remaining_partitions}"
        )

    logger.info(
        "Delete validation passed. tbl_dt partitions removed from Hive table %s: %s",
        table_name,
        tbl_dt_values
    )


def generate_date_range(start_date, end_date):
    start = datetime.strptime(str(start_date), "%Y%m%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(str(end_date), "%Y%m%d").replace(tzinfo=timezone.utc)

    current_date = start
    date_range = []

    while current_date <= end:
        date_range.append(int(current_date.strftime("%Y%m%d")))
        current_date += timedelta(days=1)

    return date_range


def run_command(cmd):
    try:
        # Run the command and capture its output as text.
        output = subprocess.check_output(cmd, shell=True, text=True)
        return output
    except subprocess.CalledProcessError as e:
        logger.warning(f"Error running command: {e}")
        return ""


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Drop tbl_dt partitions from a Hive table and clean up the "
            "corresponding HDFS data files. start-date, end-date, and "
            "table-name are all required."
        )
    )
    parser.add_argument(
        "--start-date",
        required=True,
        type=int,
        help="Start of the tbl_dt range to delete, format YYYYMMDD (e.g. 20260629).",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=int,
        help="End of the tbl_dt range to delete, format YYYYMMDD (e.g. 20260819).",
    )
    parser.add_argument(
        "--table-name",
        required=True,
        help=(
            "Hive table to operate on, format database.table "
            "(e.g. customer_analytics.url_dm)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Preview the Hive SQL and HDFS delete commands and check existing "
            "partitions without deleting any metadata or files."
        ),
    )

    args = parser.parse_args()

    if not str(args.start_date).strip():
        parser.error("start-date is required and cannot be empty.")
    if not str(args.end_date).strip():
        parser.error("end-date is required and cannot be empty.")
    if not args.table_name or not args.table_name.strip():
        parser.error("table-name is required and cannot be empty.")

    if len(str(args.start_date)) != 8 or len(str(args.end_date)) != 8:
        parser.error("start-date and end-date must be in YYYYMMDD format.")

    if args.start_date > args.end_date:
        parser.error("start-date must be less than or equal to end-date.")

    if "." not in args.table_name:
        parser.error(
            "table-name must be in the format database.table "
            "(e.g. customer_analytics.url_dm)."
        )

    return args


if __name__ == "__main__":

    args = parse_args()

    start_date = args.start_date
    end_date = args.end_date
    table_name = args.table_name  # e.g. "customer_analytics.url_dm"
    dry_run = args.dry_run

    database_name, _, bare_table_name = table_name.partition(".")

    # Generate the date range from start_date to end_date
    tbl_dt_lst = generate_date_range(start_date, end_date)

    logger.info(
        "%sDates to be deleted: \n%s",
        "[DRY RUN] " if dry_run else "",
        tbl_dt_lst
    )

    delete_hive_tbl_dt_partitions(
        table_name=table_name,
        tbl_dt_values=tbl_dt_lst,
        dry_run=dry_run
    )

    hdfs_base_path = (
        f"hdfs://ikudmadd18.mtn.com.ng:9000/warehouse/hive/"
        f"{database_name}.db/{bare_table_name}/tbl_dt"
    )

    delete_commands = [
        f"hdfs dfs -rm -r -f {hdfs_base_path}={tbl_dt}"
        for tbl_dt in tbl_dt_lst
    ]

    if dry_run:
        logger.info(
            f"[DRY RUN] Would execute {len(delete_commands)} HDFS delete commands "
            f"(showing first 10):\n{delete_commands[:10]}"
        )
        ls_commands = [
            f"hdfs dfs -ls -d {hdfs_base_path}={tbl_dt}"
            for tbl_dt in tbl_dt_lst
        ]
        logger.info("[DRY RUN] Checking which HDFS partition directories currently exist...")
        existing_dirs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_cmd = {executor.submit(run_command, cmd): cmd for cmd in ls_commands}
            for future in concurrent.futures.as_completed(future_to_cmd):
                cmd_run = future_to_cmd[future]
                try:
                    result = future.result().strip()
                    if result:
                        existing_dirs.append(result)
                except Exception as exc:
                    logger.warning(f"Command {cmd_run} generated an exception: {exc}")
        logger.info(
            f"[DRY RUN] Found {len(existing_dirs)} existing HDFS partition directories:\n"
            + ("\n".join(sorted(existing_dirs)) if existing_dirs else "None")
        )
    else:
        logger.info(f"hdfs delete commands to be executed (first 10):\n{delete_commands[:10]}")

        outputs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_cmd = {executor.submit(run_command, cmd): cmd for cmd in delete_commands}
            for future in concurrent.futures.as_completed(future_to_cmd):
                cmd_run = future_to_cmd[future]
                try:
                    result = future.result()
                    outputs.append(result)
                except Exception as exc:
                    logger.warning(f"Command {cmd_run} generated an exception: {exc}")
        logger.info(f"hdfs delete output:\n{outputs}")
