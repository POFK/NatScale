import time
import natscale as ns

cfg = ns.Config(
    nats_server="nats://127.0.0.1:4222",
    subject="hpc.tasks.*",
    timeout=3,
    retry=4,
    auto_ack=False,
)

# manually do ack
with ns.Iterator(cfg) as tasks:
    for data, done in tasks:
        print(f"{data.id} --> {data}")
        time.sleep(1)
        done()
