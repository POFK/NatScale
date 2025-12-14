import time
import natscale as ns
from natscale.task.iter import NatsIterator as Iterator

cfg = ns.Config(
    nats_server="nats://127.0.0.1:4222",
    subject="hpc.tasks.*",
    timeout=3,
    retry = 4,
    auto_ack=True,
)

with Iterator(cfg) as tasks:
    for data in tasks:
        print(f"{data.id} --> {data}")
        time.sleep(1)
