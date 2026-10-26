const CONTESTED = ["delivered", "flagged", "drop"];
const $ = (s) => document.querySelector(s);
const api = (path, payload) => fetch(path, payload ? { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(payload) } : {}).then((r) => r.json());
let selected = null;

const badge = (s) => `<span class="badge ${s}">${s}</span>`;
const esc = (s) => String(s).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

async function refresh() {
  const st = await api("/api/status");
  if (!st.ok) { $("#leaderName").textContent = "none"; return; }
  $("#leaderName").textContent = st.leader || "none";
  $("#nodes tbody").innerHTML = Object.entries(st.nodes).map(([id, n]) => `
    <tr class="${id === st.leader ? "leader" : ""}">
      <td>${id}</td><td>${n.role}</td><td>${n.term}</td><td>${n.commit}</td>
      <td>${(n.members || []).join(",")}</td><td><code>${n.url || ""}</code></td>
      <td><button data-sel="${id}">select</button><button data-kill="${id}">kill</button></td>
    </tr>`).join("");
  const read = await api("/api/read");
  renderMessages(read);
  renderProposals(read);
}

function renderMessages(read) {
  const msgs = Object.values((read.state || {}).messages || {}).sort((a, b) => a.index - b.index);
  $("#messages").innerHTML = msgs.map((m) => `
    <li class="msg ${m.tombstone ? "tombstone" : ""}">
      <div class="meta"><b>${esc(m.author)}</b> ${badge(m.state)} <code>${m.id}</code></div>
      <div class="text">${m.current_text ? esc(m.current_text) : "(no body)"}</div>
      <div class="actions">${CONTESTED.map((t) => `<button data-msg="${m.id}" data-to="${t}">${t}</button>`).join("")}</div>
      ${(m.revisions || []).length > 1 ? `<div class="revs">revisions: ${m.revisions.map(esc).join(" | ")}</div>` : ""}
    </li>`).join("") || '<li class="empty">no messages</li>';
}

function renderProposals(read) {
  const props = Object.values((read.state || {}).proposals || {});
  const members = ((read.state || {}).config || {}).members || [];
  const need = Math.floor(members.length / 2) + 1;
  $("#proposals").innerHTML = props.map((p) => `
    <li class="prop"><code>${p.id}</code> ${p.type} on ${p.msg_id} — votes ${p.votes.length}/${need}
      <select id="voter-${p.id}">${members.map((m) => `<option>${m}</option>`).join("")}</select>
      <button data-vote="${p.id}">vote</button>${p.decided ? ` <b>decided: ${p.outcome}</b>` : ""}</li>`).join("")
    || '<li class="empty">no open proposals</li>';
}

$("#send").onclick = async () => {
  const mid = crypto.randomUUID();
  const r = await api("/api/command", { type: "submit", author: $("#user").value || "anon",
    recipients: ["all"], text: $("#text").value, msg_id: mid, command_id: mid });
  if (!r.ok) alert(r.error);
  $("#text").value = "";
  refresh();
};

$("#spawn").onclick = async () => {
  const r = await api("/admin/spawn", { node_id: $("#spawnId").value, port: Number($("#spawnPort").value) || null });
  if (!r.ok) alert(r.error);
  refresh();
};

$("#kill").onclick = async () => {
  const nid = $("#kill").dataset.node;
  if (!nid) { alert("select a node first"); return; }
  const r = await api("/admin/kill", { node_id: nid });
  if (!r.ok) alert(r.error);
  refresh();
};

document.addEventListener("click", async (e) => {
  if (e.target.dataset.sel) { selected = e.target.dataset.sel; $("#kill").dataset.node = selected; }
  if (e.target.dataset.kill) {
    const r = await api("/admin/kill", { node_id: e.target.dataset.kill });
    if (!r.ok) alert(r.error);
    refresh();
  }
  if (e.target.dataset.vote) {
    const pid = e.target.dataset.vote;
    const r = await api("/api/command", { type: "vote", voter: document.getElementById("voter-" + pid).value,
      proposal_id: pid, command_id: crypto.randomUUID() });
    if (!r.ok) alert(r.error);
    refresh();
  }
  const btn = e.target.closest("button[data-to]");
  if (btn) {
    const r = await api("/api/command", { type: "propose", by: $("#user").value || "anon",
      msg_id: btn.dataset.msg, to: btn.dataset.to, proposal_id: crypto.randomUUID(), command_id: crypto.randomUUID() });
    if (!r.ok) alert(r.error);
    refresh();
  }
});

setInterval(refresh, 2000);
refresh();
