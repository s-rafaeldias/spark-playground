from pyspark.sql import SparkSession
import pyspark.sql.types as T
from pyspark import SparkConf
import pandas as pd
from typing import Iterable, Iterator
from datetime import datetime

from pyspark.sql.streaming.state import GroupStateTimeout, GroupState


def count_messages(
    key: tuple, data_iter: Iterable[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    # `key` will be a tuple of all group by keys
    # For example:
    # df.group_df("a") -> key = ("a",)
    # df.group_df("a", "b") -> key = ("a", "b")

    # Get the current state
    if state.exists:
        (count, values, min_ts, max_ts) = state.get
    else:
        count = 0
        values = []
        min_ts = datetime.now()
        max_ts = datetime.now()


    for group_df in data_iter:
        # Count number of rows for the group by key (id)
        count += len(group_df)
        # Keep track of all `values` for an `id`
        # values += group_df["value"].to_list()

        # Keep track of min and max timestamps seen during the
        # streaming job
        # TODO: add watermark conditions here for time bound state
        tmp_max_ts = group_df["ts"].max()
        tmp_min_ts = group_df["ts"].min()

        min_ts = min_ts if min_ts < tmp_min_ts else tmp_min_ts
        max_ts = max_ts if max_ts > tmp_max_ts else tmp_max_ts

    new_state = (count, values, min_ts, max_ts)
    state.update(new_state)

    # The yield DataFrame is **not** the state itself,
    # is just the output data from the streaming job
    yield pd.DataFrame(
        {
            "id": [str(key[0])],
            "count": [count],
            "values": [values],
            "min_ts": [min_ts],
            "max_ts": [max_ts],
        }
    )


spark_conf = (
    SparkConf()
    # .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .set("spark.sql.shuffle.partitions", "2")
    .setMaster("local[*]")
    .setAppName("stateful-streaming")
)

with SparkSession.builder.config(conf=spark_conf).getOrCreate() as sc:
    read_opts = {
        "kafka.bootstrap.servers": "localhost:58776",
        "subscribe": "pyspark",
        "rowsPerBatch": 50,
        "numPartitions": 2,
        "advanceMillisPerBatch": 1000,
    }

    # df = sc.readStream.format("kafka").options(**read_opts).load()
    # df = df.selectExpr("CAST(value AS STRING) as id", "current_timestamp() as t")

    df = sc.readStream.format("rate-micro-batch").options(**read_opts).load()
    df = df.selectExpr("value", "value % 6 AS id", "current_timestamp() as ts")

    df = (
        df
        # .withWatermark("timestamp", "30 minutes")
        .groupBy("id").applyInPandasWithState(
            count_messages,
            outputStructType=T.StructType(
                [
                    T.StructField("id", T.StringType(), False),
                    T.StructField("count", T.LongType(), False),
                    T.StructField("values", T.ArrayType(T.LongType()), False),
                    T.StructField("max_ts", T.TimestampType(), False),
                    T.StructField("min_ts", T.TimestampType(), False),
                ]
            ),
            stateStructType=T.StructType(
                [
                    T.StructField("count", T.LongType(), False),
                    T.StructField("values", T.ArrayType(T.LongType()), False),
                    T.StructField("max_ts", T.TimestampType(), False),
                    T.StructField("min_ts", T.TimestampType(), False),
                ]
            ),
            outputMode="update",
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
    )

    write_opts = {
        "path": "./out",
        "checkpointLocation": f"/tmp/spark-playground/checkpoint/{int(datetime.now().timestamp())}",
        "truncate": "false",
    }

    # Print data to console
    (
        df.writeStream.format("console")
        .options(**write_opts)
        .start(outputMode="update")
        .awaitTermination()
    )
