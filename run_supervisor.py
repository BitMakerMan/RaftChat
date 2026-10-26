"""Start the supervisor (web UI + spawn/kill endpoints) on a port.

  python run_supervisor.py 8200
Then open http://127.0.0.1:8200/
"""

import asyncio
import sys

from raft.supervisor import Supervisor


async def main(port):
    s = Supervisor(port=port)
    await s.start()
    print(f"supervisor on http://127.0.0.1:{s.port} (UI at /)", flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    p = int(sys.argv[1]) if len(sys.argv) > 1 else 8200
    try:
        asyncio.run(main(p))
    except KeyboardInterrupt:
        pass
