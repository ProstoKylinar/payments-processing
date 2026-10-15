"""Заглушка приёмника webhook для демонстрации (не часть сервиса).

POST /hook  -> 200, тело печатается в лог
POST /fail  -> 500 (чтобы увидеть retry с экспоненциальной задержкой и DLQ)
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        code = 500 if self.path == "/fail" else 200
        try:
            text = json.dumps(json.loads(body), ensure_ascii=False)
        except ValueError:
            text = body.decode(errors="replace")
        print(f"WEBHOOK {self.path} -> {code}: {text}", file=sys.stderr, flush=True)
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args) -> None:  # отключаем стандартный access-лог
        pass


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
