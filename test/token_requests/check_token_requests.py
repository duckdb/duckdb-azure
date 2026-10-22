#!/usr/bin/env python3
"""Count managed-identity token requests per query, against a fake IMDS and a fake blob endpoint.

Usage: check_token_requests.py <duckdb shell> <azure.duckdb_extension>

Needs only the standard library and openssl (for a throwaway TLS certificate). The storage side
is a minimal Blob service over HTTPS; the token side is an IMDS stand-in that the Azure SDK finds
through AZURE_POD_IDENTITY_AUTHORITY_HOST.
"""

import email.utils
import http.server
import json
import os
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse

SETUP = """
.output /dev/null
LOAD '{extension}';
SET azure_transport_option_type = 'curl';
CREATE SECRET mi (TYPE azure, PROVIDER managed_identity, ACCOUNT_NAME 'acct', ENDPOINT 'localhost:{port}');
.output
"""

# Each case runs in a fresh duckdb process after SETUP.
#   sql      -- statements under test
#   expected -- the output duckdb must print (csv mode, no header)
#   tokens   -- number of token requests IMDS may see
#   throttle -- IMDS answers this many requests with 429 before it serves a token
CASES = [
    dict(
        name="one query over three files fetches one token",
        sql="SELECT count(*), sum(x) FROM read_csv('az://c/*.csv');",
        expected="3,6",
        tokens=1,
    ),
    dict(
        name="two queries on one connection",
        sql="SELECT sum(x) FROM read_csv('az://c/*.csv');\nSELECT sum(x) FROM read_csv('az://c/f1.csv');",
        expected="6\n1",
        tokens=2,
    ),
    dict(
        name="IMDS throttles once, the request is retried",
        sql="SELECT sum(x) FROM read_csv('az://c/*.csv');",
        expected="6",
        tokens=2,
        throttle=1,
    ),
    dict(
        name="two secrets on one account, each path uses its own secret",
        sql=".output /dev/null\n"
        + "".join(
            f"CREATE SECRET {c} (TYPE azure, PROVIDER managed_identity, CLIENT_ID '{c}', SCOPE 'az://{c}/', "
            f"ACCOUNT_NAME 'acct', ENDPOINT 'localhost:{{port}}');\n"
            for c in ("ca", "cb")
        )
        + ".output\nSELECT sum(x) FROM read_csv(['az://ca/f1.csv', 'az://cb/f2.csv', 'az://ca/f3.csv']);",
        expected="6",
        tokens=2,
    ),
    dict(
        name="a secret replaced between queries is used with its new identity",
        sql=".output /dev/null\n"
        + "CREATE SECRET sw (TYPE azure, PROVIDER managed_identity, CLIENT_ID 'ca', SCOPE 'az://ca/', "
        + "ACCOUNT_NAME 'acct', ENDPOINT 'localhost:{port}');\n"
        + ".output\nSELECT sum(x) FROM read_csv('az://ca/f1.csv');\n.output /dev/null\n"
        + "CREATE OR REPLACE SECRET sw (TYPE azure, PROVIDER managed_identity, CLIENT_ID 'cb', SCOPE 'az://cb/', "
        + "ACCOUNT_NAME 'acct', ENDPOINT 'localhost:{port}');\n"
        + ".output\nSELECT sum(x) FROM read_csv('az://cb/f2.csv');",
        expected="1\n2",
        tokens=2,
    ),
    dict(
        name="cache entries go away at query end",
        # One secret per file, each used by its own query: an entry that outlived its query would add up
        sql=".output /dev/null\nCALL enable_logging(level = 'debug', storage = 'memory');\n"
        + "".join(
            f"CREATE SECRET s{i} (TYPE azure, PROVIDER managed_identity, SCOPE 'az://c/f{i}.csv', "
            "ACCOUNT_NAME 'acct', ENDPOINT 'localhost:{port}');\n"
            for i in (1, 2, 3)
        )
        + ".output\n"
        + "".join(f"SELECT sum(x) FROM read_csv('az://c/f{i}.csv');\n" for i in (1, 2, 3))
        + "SELECT max(regexp_extract(message, 'cache: (\\d+) entries', 1)::INT) FROM duckdb_logs "
        + "WHERE message LIKE 'azure context cache: %';",
        expected="1\n2\n3\n1",
        tokens=3,
    ),
]

# Containers that only accept the token of one managed identity (client_id == container name)
OWNED_CONTAINERS = {"ca", "cb"}

# Seconds one duckdb process may take; a hung shell must fail the job, not stall it
TIMEOUT = 120

# Variables that would send requests past the fake endpoints (checked upper and lower case)
IGNORED_ENV = [
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "IDENTITY_ENDPOINT",
    "IDENTITY_HEADER",
    "IDENTITY_SERVER_THUMBPRINT",
    "IMDS_ENDPOINT",
    "MSI_ENDPOINT",
    "MSI_SECRET",
]

FILES = {"f1.csv": b"x\n1\n", "f2.csv": b"x\n2\n", "f3.csv": b"x\n3\n"}
LAST_MODIFIED = email.utils.formatdate(usegmt=True)


class Imds(http.server.BaseHTTPRequestHandler):
    requests = 0
    throttle = 0
    lock = threading.Lock()

    def log_message(self, *args):
        pass

    def do_GET(self):
        with Imds.lock:
            Imds.requests += 1
            throttled = Imds.requests <= Imds.throttle
        client_id = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("client_id", [""])[0]
        if throttled:
            body = b'{"error":"throttled","error_description":"Temporarily throttled, too many requests"}'
            self.send_response(429)
            self.send_header("Retry-After", "1")
        else:
            body = json.dumps(
                {
                    "access_token": token_for(client_id),
                    "expires_in": "3600",
                    "expires_on": str(int(time.time()) + 3600),
                    "token_type": "Bearer",
                    "resource": "https://storage.azure.com",
                }
            ).encode()
            self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def token_for(client_id):
    return f"fake-token-{client_id}" if client_id else "fake-token"


class Blob(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _headers(self, name, length):
        self.send_header("Content-Length", str(length))
        self.send_header("Last-Modified", LAST_MODIFIED)
        self.send_header("ETag", '"0x1"')
        self.send_header("x-ms-blob-type", "BlockBlob")
        for header in ("x-ms-creation-time", "Date"):
            self.send_header(header, LAST_MODIFIED)
        for header, value in [
            ("x-ms-request-id", "r"),
            ("x-ms-version", "2023-11-03"),
            ("x-ms-server-encrypted", "true"),
            ("x-ms-lease-state", "available"),
            ("x-ms-lease-status", "unlocked"),
            ("x-ms-access-tier", "Hot"),
            ("x-ms-access-tier-inferred", "true"),
            ("Content-Type", "application/octet-stream"),
            ("Accept-Ranges", "bytes"),
        ]:
            self.send_header(header, value)

    def _not_found(self):
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _forbidden(self):
        container = self.path.split("?")[0].lstrip("/").split("/")[0]
        if container not in OWNED_CONTAINERS:
            return False
        if self.headers.get("Authorization") == f"Bearer {token_for(container)}":
            return False
        self.send_response(403)
        self.send_header("Content-Length", "0")
        self.end_headers()
        return True

    def do_HEAD(self):
        if self._forbidden():
            return
        name = self.path.split("?")[0].rsplit("/", 1)[-1]
        if name not in FILES:
            return self._not_found()
        self.send_response(200)
        self._headers(name, len(FILES[name]))
        self.end_headers()

    def do_GET(self):
        if self._forbidden():
            return
        if "comp=list" in self.path:
            blobs = "".join(
                f"<Blob><Name>{n}</Name><Properties><Last-Modified>{LAST_MODIFIED}</Last-Modified>"
                f"<Etag>0x1</Etag><Content-Length>{len(b)}</Content-Length><BlobType>BlockBlob</BlobType>"
                "</Properties></Blob>"
                for n, b in FILES.items()
            )
            body = (
                '<?xml version="1.0" encoding="utf-8"?><EnumerationResults ContainerName="c">'
                f"<Blobs>{blobs}</Blobs><NextMarker /></EnumerationResults>"
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        name = self.path.split("?")[0].rsplit("/", 1)[-1]
        data = FILES.get(name)
        if data is None:
            return self._not_found()
        start, end = 0, len(data) - 1
        byte_range = self.headers.get("x-ms-range") or self.headers.get("Range")
        if byte_range:
            first, last = byte_range.split("=")[1].split("-")
            start, end = int(first), min(int(last), end) if last else end
        chunk = data[start : end + 1]
        self.send_response(206 if byte_range else 200)
        self._headers(name, len(chunk))
        self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.end_headers()
        self.wfile.write(chunk)


def serve(handler, tls=None):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    if tls:
        server.socket = tls.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]


def main(shell, extension):
    work = tempfile.mkdtemp(prefix="az-token-")
    cert, key = os.path.join(work, "cert.pem"), os.path.join(work, "key.pem")
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=acct.localhost",
            "-addext",
            "subjectAltName=DNS:acct.localhost",
            "-keyout",
            key,
            "-out",
            cert,
        ],
        check=True,
        capture_output=True,
    )
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(cert, key)
    blob_port = serve(Blob, tls)
    imds_port = serve(Imds)
    env = dict(os.environ, AZURE_POD_IDENTITY_AUTHORITY_HOST=f"http://127.0.0.1:{imds_port}", CURL_CA_INFO=cert)
    # A proxy, or another managed identity source (App Service, Arc, Cloud Shell), would bypass the fakes
    for name in IGNORED_ENV:
        env.pop(name, None)
        env.pop(name.lower(), None)
    # Debug builds run LeakSanitizer, which reports the SDK's global curl connection pool at exit
    env.setdefault("ASAN_OPTIONS", "detect_leaks=0")

    failed = 0
    for case in CASES:
        Imds.requests, Imds.throttle = 0, case.get("throttle", 0)
        sql = SETUP.format(extension=extension, port=blob_port) + case["sql"].replace("{port}", str(blob_port))
        try:
            run = subprocess.run(
                [shell, "-unsigned", "-csv", "-noheader"],
                input=sql,
                env=env,
                capture_output=True,
                text=True,
                timeout=TIMEOUT,
            )
            output, status = (run.stdout + run.stderr).strip(), run.returncode
        except subprocess.TimeoutExpired:
            output, status = f"timed out after {TIMEOUT} s", None
        ok = status == 0 and output == case["expected"] and Imds.requests == case["tokens"]
        failed += not ok
        print(f"{'✅' if ok else '❌'} {case['name']}: tokens {Imds.requests} (want {case['tokens']})")
        if status != 0:
            print(f"   shell exit status: {status}")
        if output != case["expected"]:
            print(f"   output: {output!r}\n   want:   {case['expected']!r}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:3]))
