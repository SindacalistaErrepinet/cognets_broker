import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from urllib.parse import parse_qs, urlparse


MESSAGES = []
MESSAGES_LOCK = Lock()


class Handler(BaseHTTPRequestHandler):
    def _write_json(self, status_code, payload):
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _channel_from_path(self):
        parts = [part for part in self.path.split("/") if part]
        if len(parts) == 3 and parts[0] == "notify" and parts[1] == "node":
            return parts[2]
        if len(parts) == 2 and parts[0] == "notify":
            return parts[1]
        return None

    def log_message(self, format, *args):
        return

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._write_json(200, {"status": "ok"})
            return

        if parsed.path != "/messages":
            self._write_json(404, {"error": "not found"})
            return

        channel = parse_qs(parsed.query).get("channel", [None])[0]
        with MESSAGES_LOCK:
            messages = [
                message for message in MESSAGES if channel is None or message["channel"] == channel
            ]
        self._write_json(200, messages)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        if parsed.path != "/messages":
            self._write_json(404, {"error": "not found"})
            return

        channel = parse_qs(parsed.query).get("channel", [None])[0]
        with MESSAGES_LOCK:
            if channel is None:
                MESSAGES.clear()
            else:
                MESSAGES[:] = [message for message in MESSAGES if message["channel"] != channel]

        self.send_response(204)
        self.end_headers()

    def do_POST(self):
        channel = self._channel_from_path()
        if channel is None:
            self._write_json(404, {"error": "not found"})
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length).decode("utf-8")
        with MESSAGES_LOCK:
            MESSAGES.append(
                {
                    "channel": channel,
                    "headers": dict(self.headers.items()),
                    "body": json.loads(raw_body),
                }
            )

        self.send_response(204)
        self.end_headers()


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
