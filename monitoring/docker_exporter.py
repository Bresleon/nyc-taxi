import http.client
import json
import socket
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List

DOCKER_SOCK = "/var/run/docker.sock"


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str) -> None:
        super().__init__("localhost")
        self.socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(4.0)
        self.sock.connect(self.socket_path)


def docker_get(path: str) -> Any:
    conn = UnixHTTPConnection(DOCKER_SOCK)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        return json.loads(resp.read().decode("utf-8"))
    finally:
        conn.close()


def collect_container_metrics(container: Dict[str, Any]) -> List[str]:
    cid = container["Id"]
    names = container.get("Names", [])
    name = names[0].lstrip("/") if names else cid[:12]
    service = container.get("Labels", {}).get("com.docker.compose.service", name)
    labels = f'name="{name}",container_label_com_docker_compose_service="{service}"'

    try:
        stats = docker_get(f"/containers/{cid}/stats?stream=false")
    except Exception:
        return []

    mem = stats.get("memory_stats", {})
    usage = mem.get("usage", 0)
    sub = mem.get("stats", {})
    inactive_file = sub.get("inactive_file", sub.get("total_inactive_file", 0))
    working_set = max(0, usage - inactive_file)
    rss = sub.get("anon", sub.get("rss", working_set))

    cpu_ns = stats.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
    cpu_sec = cpu_ns / 1_000_000_000.0

    return [
        f"container_memory_working_set_bytes{{{labels}}} {working_set}",
        f"container_memory_rss{{{labels}}} {rss}",
        f"container_cpu_usage_seconds_total{{{labels}}} {cpu_sec:.6f}",
    ]


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        containers = docker_get("/containers/json")
        lines = [
            "# TYPE container_memory_working_set_bytes gauge",
            "# TYPE container_memory_rss gauge",
            "# TYPE container_cpu_usage_seconds_total counter",
        ]
        with ThreadPoolExecutor(max_workers=6) as pool:
            for metric_group in pool.map(collect_container_metrics, containers):
                lines.extend(metric_group)

        body = ("\n".join(lines) + "\n").encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 8080), MetricsHandler)
    server.serve_forever()
