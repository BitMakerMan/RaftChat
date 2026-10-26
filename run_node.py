"""Node entrypoint: python run_node.py --id n1 --port 8301 --seeds ... --supervisor ..."""

import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from raft import net
from raft.runtime import NodeRuntime
from raft.statemachine import ChatStateMachine
from raft.storage import SqliteStorage


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Raft chatroom node")
    p.add_argument("--id", required=True)
    p.add_argument("--port", type=int, default=0)
    p.add_argument("--seeds", default="", help="comma separated node URLs (bootstrap)")
    p.add_argument("--peers", default="", help="comma separated id=url pairs (dynamic discovery seed)")
    p.add_argument("--config", default="", help="comma separated member ids (initial config)")
    p.add_argument("--supervisor", default="", help="supervisor URL for self-registration")
    p.add_argument("--data-dir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
    p.add_argument("--heartbeat-ms", type=int, default=100)
    p.add_argument("--election-min-ms", type=int, default=300)
    p.add_argument("--election-max-ms", type=int, default=600)
    p.add_argument("--snapshot-threshold", type=int, default=64)
    return p.parse_args(argv)


async def main(argv=None):
    args = parse_args(argv)
    seeds = [s.strip() for s in args.seeds.split(",") if s.strip()]
    config = [c.strip() for c in args.config.split(",") if c.strip()] or [args.id]
    storage = SqliteStorage(os.path.join(args.data_dir, f"node-{args.id}.db"), args.id)
    node = NodeRuntime(args.id, storage, ChatStateMachine(args.id), initial_config=config,
                       cfg={"heartbeat_ms": args.heartbeat_ms, "election_min_ms": args.election_min_ms,
                            "election_max_ms": args.election_max_ms,
                            "snapshot_threshold": args.snapshot_threshold})
    port = await node.start()
    node.addr = net.base_url("127.0.0.1", port)
    node.core.addr = node.addr
    for pair in args.peers.split(","):
        if "=" in pair:
            nid, url = pair.split("=", 1)
            node.core.addrs[nid.strip()] = url.strip()
    print(json.dumps({"node_id": args.id, "port": port, "url": node.addr,
                      "initial_config": config, "pid": os.getpid()}), flush=True)
    if args.supervisor:
        await net.post(args.supervisor, "/admin/register", {"node_id": args.id, "url": node.addr})
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
