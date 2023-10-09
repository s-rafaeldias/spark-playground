from pyspark.sql import SparkSession
import pyspark.sql.types as T
from pyspark import SparkConf
import pandas as pd
from typing import Iterable, Iterator
from datetime import datetime
from typing import List

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
    # df.group_df("a") -> key = ("a",)
    # df.group_df("a", "b") -> key = ("a", "b")

    # Get the current state
    if state.exists:
        (count, values) = state.get
    else:
        count = 0
        values = []

    for group_df in data_iter:
        # Count number of rows for the group by key (id)
        count += len(group_df)
        # Keep track of all `values` for an `id`
        values += group_df["value"].to_list()

    new_state = (count, values)
    state.update(new_state)

    # The yield DataFrame is **not** the state itself,
    # is just the output data from the streaming job
    yield pd.DataFrame({"id": [str(key[0])], "count": [count], "values": [values]})


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
    df = df.selectExpr("value", "value % 6 AS id")

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
                ]
            ),
            stateStructType=T.StructType(
                [
                    T.StructField("count", T.LongType(), False),
                    T.StructField("values", T.ArrayType(T.LongType()), False),
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
