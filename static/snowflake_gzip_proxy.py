#!/usr/bin/env python3
"""Decompress Snowflake connector requests before forwarding to the emulator."""

from __future__ import annotations

import gzip
import http.client
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class ProxyHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/healthz":
            body = b"ok\n"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.forward()

    def do_POST(self) -> None:
        self.forward()

    def do_PUT(self) -> None:
        self.forward()

    def do_DELETE(self) -> None:
        self.forward()

    def forward(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            content_encoding = self.headers.get("Content-Encoding", "").lower()

            if content_encoding and content_encoding != "gzip":
                self.send_error(415, "Unsupported Content-Encoding")
                return
            body = self._decompress_gzip_body(body, content_encoding)
        except (ValueError, OSError):
            self.send_error(400, "Invalid request body")
            return

        excluded = {
            "connection",
            "content-encoding",
            "content-length",
            "host",
            "transfer-encoding",
        }
        upstream = http.client.HTTPConnection("127.0.0.1", 4566, timeout=60)
        try:
            upstream.putrequest(self.command, self.path)
            for name, value in self.headers.items():
                if name.lower() not in excluded:
                    upstream.putheader(name, value)
            if body:
                upstream.putheader("Content-Length", str(len(body)))
            upstream.endheaders(body or None)

            response = upstream.getresponse()
            self.send_response(response.status, response.reason)
            for name, value in response.getheaders():
                if name.lower() not in {"connection", "content-length", "transfer-encoding"}:
                    self.send_header(name, value)
            response_body = response.read()
            if self.path.startswith("/session/v1/login-request"):
                response_body = self.normalize_response(response_body, login=True)
            elif self.path.startswith("/queries/v1/query-request"):
                response_body = self.normalize_response(response_body, login=False)
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(response_body)
        except (OSError, http.client.HTTPException):
            self.send_error(502, "Snowflake emulator unavailable")
        finally:
            upstream.close()

    @staticmethod
    def _decompress_gzip_body(body: bytes, content_encoding: str) -> bytes:
        if content_encoding == "gzip" or body.startswith(b"\x1f\x8b"):
            return gzip.decompress(body)
        return body

    @staticmethod
    def normalize_response(body: bytes, *, login: bool) -> bytes:
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return body

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return body

        if login:
            parameters = data.get("parameters")
            if isinstance(parameters, list):
                for parameter in parameters:
                    if (
                        isinstance(parameter, dict)
                        and parameter.get("name") == "CLIENT_PREFETCH_THREADS"
                        and isinstance(parameter.get("value"), str)
                    ):
                        parameter["value"] = int(parameter["value"])
        else:
            rowtype = data.setdefault("rowtype", [])
            if isinstance(rowtype, list):
                for column in rowtype:
                    if isinstance(column, dict):
                        for field in ("length", "precision", "scale"):
                            column.setdefault(field, None)
            if data.get("rowset") is None:
                data["rowset"] = []

        return json.dumps(payload, separators=(",", ":")).encode()

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"{self.address_string()} - {fmt % args}", flush=True)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 4567), ProxyHandler)
    print("Snowflake gzip adapter listening on 127.0.0.1:4567", flush=True)
    server.serve_forever()
