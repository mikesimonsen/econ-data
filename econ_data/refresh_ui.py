"""The page's refresh button and status panel (plans/manual-refresh.md).

The page is public, so nothing here shows without a key stored in that browser.
To set it up on a device, open the page with #refresh-setup and paste the key.
The service checks the key on every request; it never appears in the page.
"""

SERVICE_URL = "https://econ-data-web.fly.dev"


def refresh_widget() -> str:
    return _CSS + _JS.replace("__SERVICE_URL__", SERVICE_URL)


_CSS = """
<style>
.rf-btn { margin-left: 6px; padding: 0 4px; font-size: 12px; line-height: 15px;
  vertical-align: middle; border: 1px solid var(--border); border-radius: 4px;
  background: none; color: var(--text-muted); cursor: pointer; }
.rf-btn:hover, .rf-btn:focus { color: var(--accent); border-color: var(--accent); }
.rf-panel { position: fixed; right: 16px; bottom: 16px; z-index: 1000;
  width: 340px; max-width: calc(100vw - 32px); box-sizing: border-box;
  padding: 12px 14px; background: var(--surface); color: var(--text); font-size: 14px;
  line-height: 1.4; border: 1px solid var(--border); border-radius: 8px;
  box-shadow: 0 4px 16px rgba(0,0,0,.45); }
.rf-title { font-weight: 600; padding-right: 22px; overflow-wrap: anywhere; }
.rf-stage { margin-top: 4px; color: var(--text-muted); }
.rf-msg { margin-top: 8px; overflow-wrap: anywhere; }
.rf-msg:empty { display: none; }
.rf-msg.rf-bad { color: var(--red); }
.rf-msg.rf-good { color: var(--green); }
.rf-close { position: absolute; top: 6px; right: 8px; border: 0; background: none;
  font-size: 18px; line-height: 1; color: var(--text-muted); cursor: pointer; }
.rf-panel input { width: 100%; box-sizing: border-box; margin-top: 8px; padding: 5px;
  font-size: 16px; background: var(--bg); color: var(--text);
  border: 1px solid var(--border); border-radius: 4px; }
.rf-action { margin-top: 8px; padding: 4px 10px; font-size: 14px; cursor: pointer;
  background: none; color: var(--text); border: 1px solid var(--border); border-radius: 4px; }
</style>
"""

# Server text is only ever set through textContent, never as HTML.
_JS = """
<script>
(function () {
  var SERVICE = "__SERVICE_URL__";
  var KEY_NAME = "econRefreshKey", ACTIVE_NAME = "econRefreshActive";
  var STAGES = {
    starting: "Starting a machine on Fly...",
    requested: "Starting a machine on Fly...",
    started: "Machine running...",
    waiting: "Waiting for another run to finish...",
    fetching: "Checking the source...",
    saved: "Source checked.",
    rendering: "Data saved. Rebuilding the page...",
    pushed: "Page pushed. Waiting for GitHub Pages...",
    publish_slow: "GitHub Pages hasn't deployed this yet.",
    stalled: "The job stopped responding.",
    live: "Live on the page.",
    done: "Finished.",
    failed: "Failed."
  };
  var pollTimer = null, failures = 0;

  function store(name, value) {
    try {
      if (value === null) localStorage.removeItem(name);
      else localStorage.setItem(name, value);
    } catch (e) {}
  }
  function read(name) {
    try { return localStorage.getItem(name); } catch (e) { return null; }
  }
  function el(tag, cls, text) {
    var node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text) node.textContent = text;
    return node;
  }

  var panel = null, title, stage, msg;
  function openPanel(heading) {
    if (panel) panel.remove();
    panel = el("div", "rf-panel");
    panel.setAttribute("role", "status");
    panel.setAttribute("aria-live", "polite");
    title = el("div", "rf-title", heading);
    stage = el("div", "rf-stage");
    msg = el("div", "rf-msg");
    var close = el("button", "rf-close", "\\u00d7");
    close.setAttribute("aria-label", "Close");
    close.onclick = function () {
      clearTimeout(pollTimer);
      store(ACTIVE_NAME, null);
      panel.remove();
      panel = null;
    };
    panel.appendChild(close);
    panel.appendChild(title);
    panel.appendChild(stage);
    panel.appendChild(msg);
    document.body.appendChild(panel);
  }
  function setMsg(text, kind) {
    msg.textContent = text || "";
    msg.className = "rf-msg" + (kind ? " rf-" + kind : "");
  }

  function api(path, options) {
    options = options || {};
    options.headers = { Authorization: "Bearer " + read(KEY_NAME) };
    if (options.body) options.headers["Content-Type"] = "application/json";
    return fetch(SERVICE + path, options).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (body) {
        return { status: r.status, body: body };
      });
    });
  }

  function retryButton(sid) {
    var again = el("button", "rf-action", "Try again");
    again.onclick = function () { start(sid); };
    panel.appendChild(again);
  }

  function show(sid, id, data) {
    var v = data.verdict;
    stage.textContent = STAGES[data.stage] || data.stage;
    if (data.error) setMsg(data.error, "bad");
    else if (v) setMsg(v.message,
      v.verdict === "CAPTURED" ? "good" : v.verdict === "SOURCE_ERROR" || v.verdict === "REJECTED" ? "bad" : "");

    if (data.stage === "live") {
      // The rebuilt page is a large file, so only reload once it's really there.
      if (location.search.indexOf("r=" + id) === -1) {
        location.replace(location.pathname + "?r=" + id + location.hash);
        return;
      }
      store(ACTIVE_NAME, null);
      return;
    }
    if (data.stage === "stalled" || data.stage === "failed") {
      store(ACTIVE_NAME, null);
      retryButton(sid);
      return;
    }
    if (data.stage === "publish_slow") {
      setMsg((v ? v.message + " " : "") + "The data is saved; reload later to see it.");
      return;
    }
    if (data.finished) {
      store(ACTIVE_NAME, null);
      return;
    }
    pollTimer = setTimeout(function () { poll(sid, id); }, 3000);
  }

  function poll(sid, id) {
    api("/refresh/" + id).then(function (r) {
      failures = 0;
      if (r.status === 401) return badKey();
      if (r.status === 503) {
        setMsg(r.body.error, "bad");
        pollTimer = setTimeout(function () { poll(sid, id); }, 5000);
        return;
      }
      if (r.status !== 200) {
        store(ACTIVE_NAME, null);
        setMsg(r.body.error || "This refresh can't be found.", "bad");
        return;
      }
      show(sid, id, r.body);
    }).catch(function () {
      failures += 1;
      if (failures >= 3) setMsg("Can't reach the refresh service on Fly. Still trying.", "bad");
      pollTimer = setTimeout(function () { poll(sid, id); }, 5000);
    });
  }

  function badKey() {
    store(ACTIVE_NAME, null);
    setMsg("The stored key was rejected. Open this page with #refresh-setup to re-enter it.", "bad");
  }

  function start(sid) {
    clearTimeout(pollTimer);
    openPanel(sid);
    stage.textContent = STAGES.starting;
    api("/refresh", { method: "POST", body: JSON.stringify({ series_id: sid }) }).then(function (r) {
      if (r.status === 401) return badKey();
      if (r.status !== 200 && r.status !== 202) {
        stage.textContent = STAGES.failed;
        setMsg(r.body.error || "The request was refused.", "bad");
        return;
      }
      store(ACTIVE_NAME, JSON.stringify({ id: r.body.id, sid: sid }));
      poll(sid, r.body.id);
    }).catch(function () {
      stage.textContent = STAGES.failed;
      setMsg("Can't reach the refresh service on Fly, so nothing was started.", "bad");
      retryButton(sid);
    });
  }

  function addButtons() {
    document.querySelectorAll("tr.series-row[data-sid]").forEach(function (row) {
      var cell = row.querySelector(".series-id");
      var sid = row.getAttribute("data-sid");
      if (!cell || cell.querySelector(".rf-btn")) return;
      var button = el("button", "rf-btn", "\\u21bb");
      button.type = "button";
      button.title = "Refresh " + sid + " now";
      button.setAttribute("aria-label", "Refresh " + sid + " now");
      button.onclick = function (event) {
        event.stopPropagation();  // the row's own click opens its detail view
        start(sid);
      };
      cell.appendChild(button);
    });
  }

  function setup() {
    openPanel("Set up the refresh button");
    stage.textContent = "Paste your refresh key. It stays in this browser.";
    var input = el("input");
    input.type = "password";
    input.autocomplete = "off";
    input.setAttribute("aria-label", "Refresh key");
    var save = el("button", "rf-action", "Save key");
    save.onclick = function () {
      store(KEY_NAME, input.value.trim());
      api("/auth").then(function (r) {
        if (r.status !== 200) {
          store(KEY_NAME, null);
          setMsg("That key was rejected.", "bad");
          return;
        }
        history.replaceState(null, "", location.pathname + location.search);
        addButtons();
        input.remove();
        save.remove();
        setMsg("Saved. Refresh buttons now appear next to each series.", "good");
      }).catch(function () {
        store(KEY_NAME, null);
        setMsg("Can't reach the refresh service on Fly, so the key wasn't checked.", "bad");
      });
    };
    panel.appendChild(input);
    panel.appendChild(save);
    input.focus();
  }

  function init() {
    if (location.hash === "#refresh-setup") { setup(); return; }
    if (!read(KEY_NAME)) return;
    addButtons();
    var active = null;
    try { active = JSON.parse(read(ACTIVE_NAME)); } catch (e) {}
    if (active && active.id) {
      openPanel(active.sid);
      stage.textContent = "Checking status...";
      poll(active.sid, active.id);
    }
  }
  init();
})();
</script>
"""
