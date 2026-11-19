#!/usr/bin/env python3
"""OpenFile with EXCLUSIVE_CREATE | NULL_IF_EXISTS creates a missing file and gives nullptr for an existing one.

Usage: check_null_if_exists.py <azure.duckdb_extension> [--azurite <connection string> <container>]

SQL never sets NULL_IF_EXISTS (unity_catalog does, from C++, to backfill Delta commits), so a small C++ driver
(open_driver.cpp) is built against the libduckdb.so next to the extension and opens files through DuckDB's file
system. Both file systems run against an in-process mock over plain HTTP: abfss:// (DFS: Create, Append, Flush) and
az:// (Blob: Put Block, Put Block List), plus Get Properties and Download. The mock can lose the answer to a
successful create, as a dropped connection or a 500, so that the SDK retries it. With --azurite, the az:// cases
also run against Azurite. Cases: a missing file, an existing file, EXCLUSIVE_CREATE without NULL_IF_EXISTS, a
retried create, and several concurrent creators of one file. Needs the Python standard library and a C++
compiler ($CXX, else the one in the build's CMakeCache.txt, else c++).
"""

import base64
import email.utils
import http.server
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import threading
import urllib.parse
import uuid

ACCT = "acct"


class Store:
    def __init__(self):
        self.lock = threading.Lock()
        self.files = {}  # path -> committed bytes
        self.metadata = {}  # path -> {key: value}
        self.staged = {}  # DFS: path -> {position: bytes}; Blob: path -> {block id: bytes}
        self.etag = 0
        self.writes = []  # (method, path) of every request that changed a file
        self.lose_answer = {}  # path -> "drop" | "500": lose the answer to the next successful create of path

    def next_etag(self):
        self.etag += 1
        return f'"0x{self.etag:x}"'


STORE = Store()


class Mock(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def handle_expect_100(self):
        # The SDK sends Expect: 100-continue on every PUT. With an empty body, answer with the final status only:
        # when a "100 Continue" and an error answer arrive in one read, azure-core's curl transport reads the error
        # body at a wrong offset and the open fails with a JSON parse error (an SDK bug; whether Azure triggers it
        # is unknown). With a body, the SDK waits for the 100 before it sends the body.
        if int(self.headers.get("Content-Length") or 0) == 0:
            return True
        return super().handle_expect_100()

    def log_message(self, *args):
        if os.environ.get("MOCK_LOG"):
            sys.stderr.write("mock: " + (args[0] % args[1:]) + "\n")

    def parse(self):
        url = urllib.parse.urlsplit(self.path)
        query = {k: v[0] for k, v in urllib.parse.parse_qs(url.query, keep_blank_values=True).items()}
        parts = urllib.parse.unquote(url.path).lstrip("/").split("/", 2)  # account, container, path
        return (parts[2] if len(parts) > 2 else ""), query

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def reply(self, code, body=b"", headers=None, head=False, error=None, xml=False):
        self.send_response(code)
        headers = dict(headers or {})
        headers.setdefault("Last-Modified", email.utils.formatdate(usegmt=True))
        headers.setdefault("x-ms-request-server-encrypted", "true")
        if error:
            headers["x-ms-error-code"] = error
            if not head and xml:
                body = f'<?xml version="1.0" encoding="utf-8"?><Error><Code>{error}</Code><Message>{error}</Message></Error>'
                body, headers["Content-Type"] = body.encode(), "application/xml"
            elif not head:
                body = f'{{"error":{{"code":"{error}","message":"{error}"}}}}'.encode()
                headers["Content-Type"] = "application/json"
        if "Content-Length" not in headers:
            self.send_header("Content-Length", str(0 if head else len(body)))
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()
        if not head:
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True  # the client gave up on this connection (a race loser's retry)

    def created(self, path, xml):
        """The answer to a successful create; it may be lost, as configured in STORE.lose_answer."""
        how = STORE.lose_answer.pop(path, None)
        if how == "drop":
            self.close_connection = True  # no answer at all: the SDK sees a transport error and retries
            return
        if how == "500":
            return self.reply(500, error="OperationTimedOut", xml=xml)
        self.reply(201, headers={"ETag": STORE.next_etag()})

    def blob_headers(self, path):
        headers = {
            "ETag": f'"{path}-{len(STORE.files[path])}"',
            "x-ms-blob-type": "BlockBlob",
            "x-ms-creation-time": email.utils.formatdate(usegmt=True),
            "x-ms-lease-state": "available",
            "x-ms-lease-status": "unlocked",
            "x-ms-server-encrypted": "true",
            "Content-Type": "application/octet-stream",
        }
        headers.update({f"x-ms-meta-{k}": v for k, v in STORE.metadata.get(path, {}).items()})
        return headers

    def do_HEAD(self):
        path, _ = self.parse()
        with STORE.lock:
            if path not in STORE.files:
                return self.reply(404, head=True, error="BlobNotFound")
            headers = dict(self.blob_headers(path), **{"Content-Length": str(len(STORE.files[path]))})
        self.reply(200, headers=headers, head=True)

    def do_GET(self):
        path, _ = self.parse()
        with STORE.lock:
            if path not in STORE.files:
                return self.reply(404, error="BlobNotFound", xml=True)
            data, headers = STORE.files[path], self.blob_headers(path)
        code = 200
        rng = self.headers.get("x-ms-range") or self.headers.get("Range")
        if rng:
            lo, hi = rng.split("=")[1].split("-")
            lo, hi = int(lo), min(int(hi) if hi else len(data) - 1, len(data) - 1)
            headers["Content-Range"] = f"bytes {lo}-{hi}/{len(data)}"
            data, code = data[lo : hi + 1], 206
        self.reply(code, data, headers)

    def do_PUT(self):
        path, query = self.parse()
        data = self.body()
        if query.get("resource") == "file":
            return self.dfs_create(path)
        if query.get("comp") == "block":
            with STORE.lock:
                STORE.staged.setdefault(path, {})[query["blockid"]] = data
            return self.reply(201)
        if query.get("comp") == "blocklist":
            return self.blob_commit(path, data)
        self.reply(400, error="UnsupportedOperation")

    def dfs_create(self, path):  # DFS Path - Create
        with STORE.lock:
            if self.headers.get("If-None-Match") == "*" and path in STORE.files:
                return self.reply(409, error="PathAlreadyExists")
            STORE.writes.append(("PUT", path))
            STORE.files[path] = b""
            STORE.staged[path] = {}
            props = self.headers.get("x-ms-properties") or ""
            STORE.metadata[path] = {
                k: base64.b64decode(v).decode() for k, v in (p.split("=", 1) for p in props.split(",") if p)
            }
            return self.created(path, xml=False)

    def blob_commit(self, path, data):  # Blob Put Block List
        ids = re.findall(rb"<(?:Latest|Uncommitted|Committed)>([^<]*)</", data)
        with STORE.lock:
            if self.headers.get("If-None-Match") == "*" and path in STORE.files:
                return self.reply(409, error="BlobAlreadyExists", xml=True)
            staged = STORE.staged.get(path, {})
            STORE.writes.append(("PUT", path))
            STORE.files[path] = b"".join(staged[i.decode()] for i in ids)
            STORE.metadata[path] = {
                k[len("x-ms-meta-") :]: v for k, v in self.headers.items() if k.lower().startswith("x-ms-meta-")
            }
            return self.created(path, xml=True)

    def do_PATCH(self):  # DFS Path - Update (append, flush)
        path, query = self.parse()
        data = self.body()
        with STORE.lock:
            if path not in STORE.files:
                return self.reply(404, error="PathNotFound")
            STORE.writes.append(("PATCH", path))
            position = int(query.get("position", "0"))
            if query.get("action") == "append":
                STORE.staged.setdefault(path, {})[position] = data
                return self.reply(202)
            if query.get("action") == "flush":
                content = STORE.files[path]
                for pos, chunk in sorted(STORE.staged.get(path, {}).items()):
                    content = content[:pos] + chunk
                STORE.files[path] = content[:position]
                STORE.staged[path] = {}
                return self.reply(200, headers={"ETag": STORE.next_etag()})
        self.reply(400, error="UnsupportedOperation")


def compiler(build_dir):
    if os.environ.get("CXX"):
        return os.environ["CXX"]
    cache = build_dir / "CMakeCache.txt"
    if cache.exists():
        m = re.search(r"^CMAKE_CXX_COMPILER:\w+=(.+)$", cache.read_text(errors="replace"), re.M)
        if m and m.group(1).strip():
            return m.group(1).strip()
    return "c++"


def build_driver(extension, out_dir):
    here = pathlib.Path(__file__).resolve().parent
    root = here.parent.parent
    build_dir = pathlib.Path(extension).resolve().parent.parent.parent
    libdir = build_dir / "src"
    driver = pathlib.Path(out_dir) / "open_driver"
    cmd = [compiler(build_dir), "-std=c++17", "-O1", f"-I{root / 'duckdb' / 'src' / 'include'}"]
    cmd += [str(here / "open_driver.cpp"), f"-L{libdir}", "-lduckdb", f"-Wl,-rpath,{libdir}", "-pthread"]
    subprocess.run(cmd + ["-o", str(driver)], check=True)
    return str(driver)


class Runner:
    def __init__(self, driver, extension, connection_string, tmp):
        self.driver, self.extension = driver, extension
        self.setup = pathlib.Path(tmp) / f"setup-{uuid.uuid4().hex[:8]}.sql"
        self.setup.write_text(
            "SET azure_transport_option_type = 'curl';\n"
            f"CREATE SECRET s (TYPE azure, CONNECTION_STRING '{connection_string}');\n"
        )

    def __call__(self, *args):
        proc = subprocess.run(
            [self.driver, self.extension, str(self.setup), *args], capture_output=True, text=True, timeout=120
        )
        out = proc.stdout.strip()
        if proc.returncode != 0:
            out += f" | exit={proc.returncode}"
        if proc.stderr.strip():
            out += " | stderr: " + proc.stderr.strip().replace("\n", " / ")
        return out


def cases(run, base, mock):
    """(name, got, ok) for one backend; base is a fresh directory URL ending in '/'."""
    out = []

    def check(name, got, want):
        ok = want(got) if callable(want) else got == want
        out.append((name, got, ok))

    def key(name):  # the mock's path for base + name (the container is the URL's host in every base used here)
        return urllib.parse.unquote(urllib.parse.urlsplit(base + name).path).lstrip("/")

    def mutations(name):
        with STORE.lock:
            return sum(1 for _, p in STORE.writes if p.endswith(key(name)))

    # missing file: created, and our content is what lands
    check("missing: created", run("nie", base + "missing.json", "payload"), "created")
    check("missing: content", run("cat", base + "missing.json"), "size=7 content=payload")

    # existing file: nullptr, nothing written
    run("nie", base + "existing.json", "first")
    before = mutations("existing.json") if mock else None
    check("existing: null", run("nie", base + "existing.json", "second"), "null")
    check("existing: content unchanged", run("cat", base + "existing.json"), "size=5 content=first")
    if mock:
        check("existing: no write request", str(mutations("existing.json") - before), "0")

    # EXCLUSIVE_CREATE without NULL_IF_EXISTS: creates a missing file, throws on an existing one
    check("exclusive: missing created", run("excl", base + "excl.json", "one"), "created")
    check(
        "exclusive: existing throws",
        run("excl", base + "excl.json", "two"),
        lambda got: got.startswith("error: ") and "already exists" in got and base + "excl.json" in got,
    )
    check("exclusive: content unchanged", run("cat", base + "excl.json"), "size=3 content=one")

    # the answer to a successful create is lost and the SDK retries: the retry's "already exists" is our own create
    if mock:
        for how in ("drop", "500"):
            name = f"retried-{how}.json"
            with STORE.lock:
                STORE.lose_answer[key(name)] = how
            check(f"retried create ({how}): created", run("nie", base + name, "mine"), "created")
            with STORE.lock:
                check(f"retried create ({how}): answer was lost", str(key(name) in STORE.lose_answer), "False")
            check(f"retried create ({how}): content", run("cat", base + name), "size=4 content=mine")

    # concurrent creators: exactly one wins, and the file holds that writer's content
    race = run("race", base + "race.json", "8")
    check("race: one winner", race, lambda got: got.startswith("winners=1 errors=0 winner=writer-"))
    winner = race.rsplit("winner=", 1)[-1] if "winner=" in race else "?"
    check("race: winner's content", run("cat", base + "race.json"), f"size={len(winner)} content={winner}")
    return out


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return 2
    extension, azurite = args[0], None
    if len(args) >= 4 and args[1] == "--azurite":
        azurite = (args[2], args[3])

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Mock)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    endpoint = f"http://127.0.0.1:{server.server_address[1]}/{ACCT}"
    mock_cs = (
        f"DefaultEndpointsProtocol=http;AccountName={ACCT};AccountKey=a2V5;"
        f"BlobEndpoint={endpoint};DfsEndpoint={endpoint}"
    )

    results = []
    with tempfile.TemporaryDirectory() as tmp:
        driver = build_driver(extension, tmp)
        run = Runner(driver, extension, mock_cs, tmp)
        for prefix in (f"abfss://fs@{ACCT}.dfs.core.windows.net/", "abfss://fs/", "az://fs/"):
            base = prefix + f"t-{uuid.uuid4().hex[:8]}/"
            results += [(f"mock {prefix}: {n}", g, ok) for n, g, ok in cases(run, base, mock=True)]
        if azurite:
            connection_string, container = azurite
            run = Runner(driver, extension, connection_string, tmp)
            base = f"az://{container}/null-if-exists-{uuid.uuid4().hex[:8]}/"
            results += [(f"azurite az://: {n}", g, ok) for n, g, ok in cases(run, base, mock=False)]
    server.shutdown()

    failures = 0
    for name, got, ok in results:
        print(f"{'ok  ' if ok else 'FAIL'} {name}" + ("" if ok else f"\n  got: {got!r}"))
        failures += not ok
    print(f"{len(results) - failures}/{len(results)} passed" + ("" if azurite else "; Azurite not run (no --azurite)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
