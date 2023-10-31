import sys
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark import SparkConf
import pandas as pd
from typing import Iterable, Iterator, Tuple
from datetime import datetime

from pyspark.sql.streaming.state import GroupStateTimeout, GroupState

LOOKUP = {
    "ABC": {"timeout": 15_000, "threshold": 5},
    "XYZ": {"timeout": 10_000, "threshold": 2},
}


def alert_func(
    key: Tuple, data_iter: Iterable[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    if not state.hasTimedOut:
        (sensor_id,) = key
        print(f"New data for {sensor_id}")

        if state.exists:
            (count, start_ts) = state.get
        else:
            count = 0
            start_ts = datetime.now()

        for group_df in data_iter:
            count += len(group_df)
            start_ts = min(group_df["ts"].min(), start_ts)

        if count >= LOOKUP[sensor_id]["threshold"]:
            print(
                f"{sensor_id}: ALERTTTTTTTTT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            )

        state.update((count, start_ts))
        # For each new "bad" data, we increase the timeout window
        state.setTimeoutDuration(LOOKUP[sensor_id]["timeout"])
        return

    (sensor_id,) = key
    (count, start_ts) = state.get

    triggered = count >= LOOKUP[sensor_id]["threshold"]
    if triggered:
        print(
            f"TIMEOUT {sensor_id}: ALERTTTTTTTTT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        )

    # Clear state
    state.remove()
    end_ts = datetime.now()

    print("Saving data to DELTA...")
    yield pd.DataFrame(
        {
            "sensor_id": [sensor_id],
            "count": [count],
            "start_ts": [start_ts],
            "end_ts": [end_ts],
            "timeout_window": [end_ts.timestamp() - start_ts.timestamp()],
            "threshold": [LOOKUP[sensor_id]["threshold"]],
            "triggered": [triggered],
            "timeout_ms": [LOOKUP[sensor_id]["timeout"]],
        }
    )


def main(kafka_port: str):
    spark_conf = (
        SparkConf()
        .set(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-core_2.12:2.4.0",
        )
        .set("spark.sql.shuffle.partitions", "2")
        .set(
            "spark.sql.streaming.statefulOperator.checkCorrectness.enabled",
            "false",
        )
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .setMaster("local[*]")
        .setAppName("stateful-streaming")
    )

    with SparkSession.builder.config(conf=spark_conf).getOrCreate() as sc:
        read_opts = {
            "kafka.bootstrap.servers": f"localhost:{int(kafka_port)}",
            "subscribe": "example",
        }

        schema = T.StructType(
            [
                T.StructField("event", T.StringType(), False),
                T.StructField("sensor_id", T.StringType(), False),
                T.StructField("ts", T.TimestampType(), False),
            ]
        )

        df = sc.readStream.format("kafka").options(**read_opts).load()
        df = (
            df.withColumn("value", F.col("value").cast(T.StringType()))
            .withColumn("value", F.from_json("value", schema))
            .select("value.*")
        )

        df = (
            df.filter(F.col("event") == "bad")
            .groupBy("sensor_id")
            .applyInPandasWithState(
                alert_func,
                outputStructType=T.StructType(
                    [
                        T.StructField("sensor_id", T.StringType(), False),
                        T.StructField("count", T.IntegerType(), False),
                        T.StructField("threshold", T.IntegerType(), True),
                        T.StructField("triggered", T.BooleanType(), True),
                        T.StructField("start_ts", T.TimestampType(), True),
                        T.StructField("end_ts", T.TimestampType(), True),
                        T.StructField("timeout_window", T.DoubleType(), True),
                        T.StructField("timeout_ms", T.IntegerType(), True),
                    ]
                ),
                stateStructType=T.StructType(
                    [
                        T.StructField("count", T.IntegerType(), False),
                        T.StructField("start_ts", T.TimestampType(), False),
                    ]
                ),
                outputMode="append",
                timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
            )
        )

        x = int(datetime.now().timestamp())
        write_opts = {
            "path": "./table",
            "checkpointLocation": f"/tmp/spark-playground/checkpoint/{x}",
            "truncate": "false",
        }

        # Print data to console
        (
            df.writeStream.format("delta")
            .options(**write_opts)
            .start(outputMode="append")
            .awaitTermination()
        )


if __name__ == "__main__":
    kafka_port = sys.argv[1]
    main(kafka_port)
