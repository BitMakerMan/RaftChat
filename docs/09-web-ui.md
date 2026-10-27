# 09 — Web UI

`web/` is a static page served by the supervisor. It has no build step and no dependencies:
`index.html`, `app.js`, `style.css`. It talks only to the supervisor.

Open `http://127.0.0.1:8200/`.

## Endpoints the UI may call

```text
GET  /api/status
GET  /api/read
POST /api/command
POST /admin/spawn
POST /admin/kill
```

`tests/test_webui.py` asserts that this set is exactly what `app.js` uses, so the UI cannot drift
onto an endpoint the supervisor does not serve.

## Sections

| Area | Elements | Behaviour |
|---|---|---|
| header | `#leader`, `#leaderName` | shows the current leader from `/api/status` |
| admin | `#spawnId`, `#spawnPort`, `#spawn`, `#kill`, `#hint` | spawn a node; kill the node selected in the table |
| cluster | `#nodes` table | one row per discovered node: node, role, term, `commit_index`, members, `addr`; the leader row is highlighted; row buttons `select` and `kill` |
| chat | `#user`, `#text`, `#send`, `#messages` | composer and message list |
| decisions | `#proposals` | proposals with votes, required quorum, voter selector, vote button |

The UI refreshes every 2 seconds (`setInterval(refresh, 2000)`) and once on load.

## What the UI does on click

- **send** → `POST /api/command` with `type: submit`, `msg_id` and `command_id` both set to a
  generated UUID, `recipients: ["all"]`.
- **message action buttons** → `POST /api/command` with `type: propose`, target from
  `CONTESTED = ["delivered", "ack", "flagged", "resolved", "drop"]`. Direct transitions such as
  `read` are not exposed as buttons because they are not contested decisions; they are available
  through the API.
- **vote** → `POST /api/command` with `type: vote`, `voter` from the proposal's member select.
- **spawn** → `POST /admin/spawn` with `node_id` and optional `port`.
- **kill** → `POST /admin/kill` with the selected node id (`#kill` uses `dataset.node`, set by the
  `select` button), or directly with the row's kill button.

Quorum display uses the active config from the read state:

```js
const members = ((read.state || {}).config || {}).members || [];
const need = Math.floor(members.length / 2) + 1;
```

## Guards

`tests/test_webui.py` checks, without a browser:

1. static files are served with the right content type;
2. every `<button id="...">` in `index.html` has a handler in `app.js`;
3. every `$("#id")` selector in `app.js` exists in `index.html`;
4. the UI only calls endpoints the supervisor serves;
5. every field `app.js` renders exists in the real payloads (`/node/status`, the read response,
   message records, proposal records) — this is what caught `n.commit` vs `commit_index` and
   `n.url` vs `addr`;
6. a live end-to-end flow: spawn, send, propose, vote to quorum, read, spawn a fourth node, kill
   it.
