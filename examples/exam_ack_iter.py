import time
import natscale as ns

cfg = ns.Config(
    nats_server="nats://127.0.0.1:4222",
    subject="hpc.tasks.*",
    timeout=60,
    auto_ack=False,
)

# manually do ack
for data, done in ns.Iterator(cfg):
    print(f"{data.id} --> {data}")
    time.sleep(3)
    done()
