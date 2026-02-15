import time
import natscale as ns
from loguru import logger

task = "data"
#task = "notseen"
prefix = "natscale.tasks"
subject = ".".join([prefix, task])

cfg = ns.Config(
    nats_server="nats://127.0.0.1:4222",
    subject=subject,
    durable_name=task,
    timeout=5,
    retry=6,
    auto_ack=False,
)

# manually do ack
with ns.Iterator(cfg) as tasks:
    for data, done in tasks:
        logger.info(f"{data.id} --> {data}")


logger.success(f"All Done")
