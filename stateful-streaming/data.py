import sys
from pydantic import BaseModel
from uuid import uuid4
import random
from confluent_kafka import Producer
from enum import StrEnum, auto
from datetime import datetime


class EventType(StrEnum):
    NORMAL = auto()
    BAD = auto()


class SensorEvent(BaseModel):
    event: EventType
    sensor_id: str
    ts: datetime


def generate_data(n: int, t: EventType) -> [SensorEvent]:
    sensor = random.choice(("ABC", "XYZ"))
    result = []

    for i in range(n):
        result.append(SensorEvent(event=t, sensor_id=sensor, ts=datetime.now()))

    return result


def main(port: str, n_runs: int, t: EventType):
    data = generate_data(n_runs, t)

    p = Producer(
        {"bootstrap.servers": f"localhost:{int(port)}", "client.id": "data-producer"}
    )

    for d in data:
        p.produce(topic="example", key=str(uuid4()), value=d.model_dump_json())
        print(d.model_dump_json())

    p.flush()


if __name__ == "__main__":
    port = sys.argv[1]
    n_runs = int(sys.argv[2])
    t = sys.argv[3]

    main(port=port, n_runs=n_runs, t=EventType(t))
