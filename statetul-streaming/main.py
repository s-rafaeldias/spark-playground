from pyspark.sql import SparkSession
from pyspark import SparkConf
import pandas as pd
from typing import Iterable, Iterator
from datetime import datetime

from pyspark.sql.streaming.state import GroupStateTimeout, GroupState


def count_messages(
    key: tuple, data_iter: Iterable[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    """
    Count how many messages for each id has been processed during
    the lifetime of the streaming job.
    """
    # `key` will be a tuple of all group by keys
    # For example:
    # df.group_df("a", "b") -> key = ("a", "b")

    count = 0
    for group_df in data_iter:
        count += len(group_df)

    # Get the current state
    if state.exists:
        (s_count,) = state.get
        count += s_count

    state.update((count,))

    # The yield DataFrame is **not** the state itself,
    # is just the output data from the streaming job
    yield pd.DataFrame({"id": [str(key[0])], "count": [count]})


spark_conf = (
    SparkConf()
    .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
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
    df = df.selectExpr("value", "value % 6 AS id")

    df = (
        df
        # .withWatermark("timestamp", "30 minutes")
        .groupBy("id")
        .applyInPandasWithState(
            count_messages,
            outputStructType="id string, count long",
            stateStructType="count long",
            outputMode="update",
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
    )

    # Print data to console
    write_opts = {
        "path": "./out",
        "checkpointLocation": f"/tmp/spark-playground/checkpoint/{int(datetime.now().timestamp())}",
        "truncate": "false",
    }

    (
        df
        .writeStream
        .format("console")
        .options(**write_opts)
        .start(outputMode="update")
        .awaitTermination()
    )
