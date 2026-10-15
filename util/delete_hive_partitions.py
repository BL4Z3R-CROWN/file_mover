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


def create_spark_session(table_name):
    """
    Create a SparkSession that supports BOTH native Hive tables and
    Iceberg tables registered in the Hive Metastore.
    """
    spark = SparkSession \
        .builder \
        .appName(f"purge_partitions:{table_name}") \
        .config("spark.driver.maxResultSize", "256g") \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .config("spark.sql.parquet.writeLegacyFormat", "false") \
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.warehouse", "hdfs://10.1.154.3:9000/warehouse/hive") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://10.1.154.3:9000") \
        .config("hive.metastore.uris", "thrift://10.1.154.38:9083") \
        .config("spark.sql.warehouse.dir", "hdfs://10.1.154.3:9000/warehouse/hive") \
        .config("spark.sql.shuffle.partitions", 200) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.parquet.enableVectorizedWriter", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('OFF')
    return spark


def inspect_table_metadata(spark, table_name):
    """
    Inspect DESCRIBE FORMATTED from the Hive Metastore and detect whether
    the table is an Iceberg table (table_type=ICEBERG) or a standard Hive table.
    """
    is_iceberg = False
    location = None
    meta_lines = []

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
        for row in desc_rows:
            col_name = (row[0] or "").strip().rstrip(":")
            data_type = (row[1] or "").strip()
            if col_name in interesting_keys:
                meta_lines.append(f"  {col_name}: {data_type}")
            if col_name == "Location" and data_type:
                location = data_type
            if col_name == "Table Properties" and (
                "TABLE_TYPE=ICEBERG" in data_type.upper()
                or "CURRENT-SNAPSHOT-ID" in data_type.upper()
            ):
                is_iceberg = True
            if col_name == "Provider" and data_type.lower() == "iceberg":
                is_iceberg = True

        if meta_lines:
            logger.info(
                "Table metadata for %s:\n%s",
                table_name,
                "\n".join(meta_lines)
            )
    except Exception as exc:
        logger.warning(f"Could not inspect Hive metadata for {table_name}: {exc}")

    return is_iceberg, location


def get_matching_hive_partitions(spark, table_name, tbl_dt_values):
    """
    Use SHOW PARTITIONS for native Hive tables.
    """
    target_set = {str(dt) for dt in tbl_dt_values}
    partitions_df = spark.sql(f"SHOW PARTITIONS {table_name}")
    all_partitions = [row[0] for row in partitions_df.collect()]

    matched_partitions = []
    for part_spec in all_partitions:
        for token in part_spec.split("/"):
            if token.startswith("tbl_dt="):
                val = token.split("=", 1)[1].strip("'\"")
                if val in target_set:
                    matched_partitions.append(part_spec)
                break

    return sorted(matched_partitions)


def handle_iceberg_table(
    spark,
    table_name,
    tbl_dt_values,
    dry_run=False,
    expire_snapshots=False,
    remove_orphans=False
):
    iceberg_table_name = f"iceberg.{table_name}"
    partition_filter = ", ".join(str(x) for x in tbl_dt_values)

    logger.info(
        "%sDetected table %s is an ICEBERG table in Hive Metastore (using %s)",
        "[DRY RUN] " if dry_run else "",
        table_name,
        iceberg_table_name
    )

    delete_sql = f"""
        DELETE FROM {iceberg_table_name}
        WHERE tbl_dt IN ({partition_filter})
    """

    # Fast metadata check using Iceberg's .partitions metadata table
    partitions_meta_sql = f"""
        SELECT partition.tbl_dt AS tbl_dt, record_count, file_count
        FROM {iceberg_table_name}.partitions
        WHERE partition.tbl_dt IN ({partition_filter})
        ORDER BY tbl_dt
    """

    if dry_run:
        logger.info(f"[DRY RUN] Would execute Iceberg DELETE SQL:\n{delete_sql}")
        logger.info(
            "[DRY RUN] Querying Iceberg metadata table (%s.partitions) for existing partitions...",
            iceberg_table_name
        )
        try:
            existing_df = spark.sql(partitions_meta_sql)
            rows = existing_df.collect()
            logger.info(f"[DRY RUN] Matching partitions currently in Iceberg metadata: {len(rows)}")
            if rows:
                existing_df.show(len(rows), truncate=False)
        except Exception as exc:
            logger.warning(
                f"Could not query {iceberg_table_name}.partitions metadata table ({exc}); "
                "falling back to SELECT COUNT(*)"
            )
            fallback_sql = f"""
                SELECT tbl_dt, COUNT(*) AS row_count
                FROM {iceberg_table_name}
                WHERE tbl_dt IN ({partition_filter})
                GROUP BY tbl_dt
                ORDER BY tbl_dt
            """
            existing_df = spark.sql(fallback_sql)
            existing_count = existing_df.count()
            logger.info(f"[DRY RUN] Matching partitions currently in Iceberg table: {existing_count}")
            if existing_count > 0:
                existing_df.show(existing_count, truncate=False)
        return

    # 1. Execute Iceberg DELETE
    logger.info(f"Running Iceberg delete:\n{delete_sql}")
    spark.sql(delete_sql)

    # 2. Validate delete
    check_sql = f"""
        SELECT tbl_dt, COUNT(*) AS row_count
        FROM {iceberg_table_name}
        WHERE tbl_dt IN ({partition_filter})
        GROUP BY tbl_dt
    """
    logger.info("Validating deleted partitions:\n%s", check_sql)
    remaining_df = spark.sql(check_sql)
    remaining_count = remaining_df.count()
    logger.info(f"remaining_count after deletion: {remaining_count}")

    if remaining_count > 0:
        logger.error(
            "Delete validation failed. Some tbl_dt values still exist in %s",
            iceberg_table_name
        )
        remaining_df.show(truncate=False)
        raise RuntimeError(
            f"Delete validation failed for tbl_dt values: {tbl_dt_values}"
        )

    logger.info(
        "Delete validation passed. tbl_dt partitions removed from current Iceberg snapshot: %s",
        tbl_dt_values
    )

    # 3. Optional Iceberg snapshot expiration & orphan cleanup
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    if expire_snapshots:
        expire_sql = f"""
            CALL iceberg.system.expire_snapshots(
                table => '{table_name}',
                older_than => TIMESTAMP '{now}',
                retain_last => 1,
                stream_results => true
            )
        """
        logger.info("Expiring Iceberg snapshots:\n%s", expire_sql)
        spark.sql(expire_sql).show(truncate=False)

    if remove_orphans:
        remove_orphan_sql = f"""
            CALL iceberg.system.remove_orphan_files(
                table => '{table_name}',
                dry_run => false
            )
        """
        logger.info("Removing Iceberg orphan files:\n%s", remove_orphan_sql)
        spark.sql(remove_orphan_sql).show(truncate=False)


def handle_native_hive_table(
    spark,
    table_name,
    tbl_dt_values,
    dry_run=False,
    batch_size=100
):
    logger.info(
        "%sDetected table %s is a NATIVE HIVE table.",
        "[DRY RUN] " if dry_run else "",
        table_name
    )

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
        output = subprocess.check_output(cmd, shell=True, text=True)
        return output
    except subprocess.CalledProcessError as e:
        logger.warning(f"Error running command: {e}")
        return ""


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Delete tbl_dt partitions from a Hive or Iceberg table and clean up "
            "the corresponding HDFS data files. Automatically detects whether "
            "the table in the Hive Metastore is native Hive or Iceberg."
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
            "Table to operate on, format database.table "
            "(e.g. customer_analytics.url_dm)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Preview the SQL and HDFS delete commands and check existing "
            "partitions without deleting any metadata or files."
        ),
    )
    parser.add_argument(
        "--expire-snapshots",
        action="store_true",
        help="(Iceberg only) Run expire_snapshots procedure after deleting partitions.",
    )
    parser.add_argument(
        "--remove-orphans",
        action="store_true",
        help="(Iceberg only) Run remove_orphan_files procedure after deleting partitions.",
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

    tbl_dt_lst = generate_date_range(start_date, end_date)

    logger.info(
        "%sDates to be deleted: \n%s",
        "[DRY RUN] " if dry_run else "",
        tbl_dt_lst
    )

    spark = create_spark_session(table_name)
    is_iceberg, table_location = inspect_table_metadata(spark, table_name)

    if is_iceberg:
        handle_iceberg_table(
            spark=spark,
            table_name=table_name,
            tbl_dt_values=tbl_dt_lst,
            dry_run=dry_run,
            expire_snapshots=args.expire_snapshots,
            remove_orphans=args.remove_orphans,
        )
        # Iceberg tables store partition folders under /data/tbl_dt=<dt>
        hdfs_base_path = (
            f"hdfs://ikudmadd18.mtn.com.ng:9000/warehouse/hive/"
            f"{database_name}.db/{bare_table_name}/data/tbl_dt"
        )
    else:
        handle_native_hive_table(
            spark=spark,
            table_name=table_name,
            tbl_dt_values=tbl_dt_lst,
            dry_run=dry_run,
        )
        # Native Hive tables store partition folders directly under /tbl_dt=<dt>
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
