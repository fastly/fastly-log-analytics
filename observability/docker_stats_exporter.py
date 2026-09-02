"""Minimal Prometheus exporter for per-container CPU/mem/net stats.

Exists because cAdvisor can't run on this engine: its containerd-backed
image store has no classic overlayfs layerdb, and cAdvisor's docker handler
aborts container registration entirely when it can't resolve a layer ID
(see docker-compose.observability.yml). The Docker Engine API itself has no
such dependency — `docker stats` works fine here — so this talks to it
directly over the daemon socket. Stdlib only: no image pull, no pip install.
"""

from __future__ import annotations

import http.client
import http.server
import json
import socket
import threading
import time

DOCKER_SOCK = "/var/run/docker.sock"
POLL_INTERVAL_S = 5
PORT = 9417

_lock = threading.Lock()
_metrics_text = "# no scrape yet\n"


class UnixSocketConnection(http.client.HTTPConnection):
    def __init__(self, path: str) -> None:
        super().__init__("localhost")
        self._sock_path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self._sock_path)


def _docker_get(path: str) -> dict:
    conn = UnixSocketConnection(DOCKER_SOCK)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read()
    finally:
        conn.close()
    return json.loads(body)


def _cpu_percent(stats: dict) -> float:
    cpu = stats.get("cpu_stats", {})
    precpu = stats.get("precpu_stats", {})
    cpu_delta = cpu.get("cpu_usage", {}).get("total_usage", 0) - precpu.get("cpu_usage", {}).get("total_usage", 0)
    sys_delta = cpu.get("system_cpu_usage", 0) - precpu.get("system_cpu_usage", 0)
    online_cpus = cpu.get("online_cpus") or len(cpu.get("cpu_usage", {}).get("percpu_usage") or [1])
    if sys_delta <= 0 or cpu_delta < 0:
        return 0.0
    return (cpu_delta / sys_delta) * online_cpus


def _collect_once() -> str:
    lines = [
        "# HELP docker_container_cpu_usage_ratio Fraction of one CPU core consumed (1.0 = 1 core).",
        "# TYPE docker_container_cpu_usage_ratio gauge",
        "# HELP docker_container_memory_usage_bytes Current memory usage (cgroup accounting).",
        "# TYPE docker_container_memory_usage_bytes gauge",
        "# HELP docker_container_memory_limit_bytes Memory limit (0 = unbounded).",
        "# TYPE docker_container_memory_limit_bytes gauge",
        "# HELP docker_container_network_receive_bytes_total Cumulative bytes received.",
        "# TYPE docker_container_network_receive_bytes_total counter",
        "# HELP docker_container_network_transmit_bytes_total Cumulative bytes transmitted.",
        "# TYPE docker_container_network_transmit_bytes_total counter",
    ]
    try:
        containers = _docker_get("/containers/json")
    except (OSError, json.JSONDecodeError):
        return "\n".join(lines) + "\n"

    for c in containers:
        cid = c.get("Id", "")
        name = (c.get("Names") or ["/?"])[0].lstrip("/")
        try:
            stats = _docker_get(f"/containers/{cid}/stats?stream=false")
        except (OSError, json.JSONDecodeError):
            continue

        mem = stats.get("memory_stats", {})
        mem_usage = mem.get("usage", 0)
        # cgroup v2 folds page cache into `usage`; subtract it so this tracks
        # cAdvisor's working_set semantics instead of "usage incl. cache".
        inactive_file = mem.get("stats", {}).get("inactive_file", 0)
        mem_usage = max(mem_usage - inactive_file, 0)
        mem_limit = mem.get("limit", 0)

        rx = tx = 0
        for net in (stats.get("networks") or {}).values():
            rx += net.get("rx_bytes", 0)
            tx += net.get("tx_bytes", 0)

        labels = f'name="{name}",id="{cid[:12]}"'
        lines.append(f"docker_container_cpu_usage_ratio{{{labels}}} {_cpu_percent(stats):.6f}")
        lines.append(f"docker_container_memory_usage_bytes{{{labels}}} {mem_usage}")
        lines.append(f"docker_container_memory_limit_bytes{{{labels}}} {mem_limit}")
        lines.append(f"docker_container_network_receive_bytes_total{{{labels}}} {rx}")
        lines.append(f"docker_container_network_transmit_bytes_total{{{labels}}} {tx}")

    return "\n".join(lines) + "\n"


def _poll_loop() -> None:
    global _metrics_text
    while True:
        text = _collect_once()
        with _lock:
            _metrics_text = text
        time.sleep(POLL_INTERVAL_S)


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 — http.server's required method name
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        with _lock:
            body = _metrics_text.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:  # silence per-request access log
        pass


if __name__ == "__main__":
    threading.Thread(target=_poll_loop, daemon=True).start()
    http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
