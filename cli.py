"""CLI client for the Raft chatroom.

Examples:
  python cli.py send --supervisor http://127.0.0.1:8200 --user alice --to bob --text "ciao"
  python cli.py read --supervisor http://127.0.0.1:8200
  python cli.py status
  python cli.py propose --user n1 --msg m1 --state delivered --proposal p1
  python cli.py vote --voter n1 --proposal p1
  python cli.py spawn --id n4
  python cli.py kill --id n4 [--force]
"""

import argparse
import asyncio
import json
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from raft import net


async def call(base, path, payload=None, timeout=10.0):
    if payload is None:
        return await net.get(base, path, timeout=timeout)
    return await net.post(base, path, payload, timeout=timeout)


async def main():
    ap = argparse.ArgumentParser(description="Raft chatroom client")
    ap.add_argument("--supervisor", default=os.environ.get(
        "RAFT_SUPERVISOR", "http://127.0.0.1:8200"))
    ap.add_argument("--node", default=os.environ.get("RAFT_NODE"))
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("send")
    s.add_argument("--user", required=True)
    s.add_argument("--to", default="all")
    s.add_argument("--text", required=True)
    s.add_argument("--cid", default=None)
    s.add_argument("--msg", default=None)

    sub.add_parser("read")
    sub.add_parser("status")

    p = sub.add_parser("propose")
    p.add_argument("--user", required=True)
    p.add_argument("--msg", required=True)
    p.add_argument("--state", required=True, help="contested target state")
    p.add_argument("--proposal", default=None)

    v = sub.add_parser("vote")
    v.add_argument("--voter", required=True)
    v.add_argument("--proposal", required=True)

    sp = sub.add_parser("spawn")
    sp.add_argument("--id", required=True, help="node id to spawn")
    sp.add_argument("--port", type=int, default=None)

    k = sub.add_parser("kill")
    k.add_argument("--id", required=True, help="node id to kill")
    k.add_argument("--force", action="store_true")

    args = ap.parse_args()
    base = args.node or args.supervisor

    if args.cmd == "send":
        payload = {"type": "submit", "author": args.user,
                   "recipients": ["all"] if args.to == "all" else [args.to],
                   "text": args.text, "command_id": args.cid or str(uuid.uuid4())}
        if args.msg:
            payload["msg_id"] = args.msg
    elif args.cmd == "propose":
        payload = {"type": "propose", "by": args.user, "msg_id": args.msg,
                   "to": args.state, "proposal_id": args.proposal or str(uuid.uuid4()),
                   "command_id": str(uuid.uuid4())}
    elif args.cmd == "vote":
        payload = {"type": "vote", "voter": args.voter, "proposal_id": args.proposal,
                   "command_id": str(uuid.uuid4())}
    elif args.cmd in ("spawn", "kill"):
        payload = {"node_id": args.id, "port": getattr(args, "port", None),
                   "force": getattr(args, "force", False)}
    else:
        payload = None

    if args.cmd == "read":
        res = await call(base, "/api/read")
    elif args.cmd in ("status", "read") or payload is None:
        res = await call(base, "/api/status")
    elif args.cmd in ("spawn", "kill"):
        res = await call(base, f"/admin/{args.cmd}", payload)
    else:
        res = await call(base, "/api/command", payload)

    print(json.dumps(res, indent=2, sort_keys=True, default=str))
    inner = (res or {}).get("result") or {}
    if isinstance(inner, dict) and inner.get("ok") is False:
        return 1
    return 0 if (res or {}).get("ok") else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
