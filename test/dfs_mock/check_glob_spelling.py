#!/usr/bin/env python3
"""A DFS glob returns its results in the spelling of its pattern (duckdb-azure#211).

Usage: check_glob_spelling.py <duckdb shell> <azure.duckdb_extension>

Azurite has no DFS endpoint, so this runs against a minimal ADLS Gen2 mock over plain HTTP that serves
the calls a glob and a read make: Path - List (JSON), List Blobs (XML), Get Properties (HEAD) and a
ranged Download. A connection string (BlobEndpoint/DfsEndpoint) points the SDK at the mock; the URLs in
SQL keep their real <container>@<account>.dfs.core.windows.net and <account>.dfs.core.windows.net/<container>
spellings. Needs only the Python standard library.
"""

import email.utils
import http.server
import json
import subprocess
import sys
import threading
import urllib.parse
from xml.sax.saxutils import escape

ACCT = "acct"
FILES = {
    "top.csv": b"x\n0\n",
    "data/a.csv": b"x\n1\n",
    "data/sub/b.csv": b"x\n2\n",
    "data/sub/deep/c.csv": b"x\n3\n",
    "sp ace/d.csv": b"x\n4\n",
    "pct%20dir/e.csv": b"x\n5\n",
    "uni/\u00fcn\u00ef/f.csv": b"x\n6\n",
    "lake/main/t/ducklake-0001.parquet": b"PAR1",
    "lake/main/t/ducklake-0002.parquet": b"PAR1",
}
DIRS = {"/".join(f.split("/")[:i]) for f in FILES for i in range(1, f.count("/") + 1)}
NOW = email.utils.formatdate(usegmt=True)
BLOB_HEADERS = {
    "Last-Modified": NOW,
    "ETag": '"0x1"',
    "x-ms-blob-type": "BlockBlob",
    "x-ms-creation-time": NOW,
    "x-ms-server-encrypted": "true",
    "x-ms-lease-state": "available",
    "x-ms-lease-status": "unlocked",
    "Content-Type": "application/octet-stream",
}


class Mock(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def parse(self):
        url = urllib.parse.urlsplit(self.path)
        query = {k: v[0] for k, v in urllib.parse.parse_qs(url.query, keep_blank_values=True).items()}
        parts = urllib.parse.unquote(url.path).lstrip("/").split("/", 2)  # account, container, path
        return (parts[2] if len(parts) > 2 else ""), query

    def reply(self, code, body=b"", headers=None, head=False):
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        if not head or "Content-Length" not in (headers or {}):
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head:
            self.wfile.write(body)

    def not_found(self, head=False):
        self.reply(
            404,
            b"" if head else b'{"error":{"code":"PathNotFound","message":"not found"}}',
            {"x-ms-error-code": "PathNotFound", "Content-Type": "application/json"},
            head,
        )

    def do_HEAD(self):
        path, _ = self.parse()
        if path in FILES:
            self.reply(200, headers=dict(BLOB_HEADERS, **{"Content-Length": str(len(FILES[path]))}), head=True)
        elif path.rstrip("/") in DIRS:
            self.reply(200, headers=dict(BLOB_HEADERS, **{"x-ms-meta-hdi_isfolder": "true"}), head=True)
        else:
            self.not_found(head=True)

    def do_GET(self):
        path, query = self.parse()
        if query.get("resource") == "filesystem":
            return self.list_paths(query)
        if query.get("comp") == "list":
            return self.list_blobs(query)
        if path not in FILES:
            return self.not_found()
        data, code, headers = FILES[path], 200, dict(BLOB_HEADERS)
        rng = self.headers.get("x-ms-range") or self.headers.get("Range")
        if rng:
            lo, hi = rng.split("=")[1].split("-")
            lo, hi = int(lo), min(int(hi) if hi else len(data) - 1, len(data) - 1)
            headers["Content-Range"] = f"bytes {lo}-{hi}/{len(data)}"
            data, code = data[lo : hi + 1], 206
        self.reply(code, data, headers)

    def list_paths(self, query):
        directory = query.get("directory", "").strip("/")
        if directory and directory not in DIRS:
            return self.not_found()
        prefix = directory + "/" if directory else ""
        entries = [(n, True) for n in sorted(DIRS) if n.startswith(prefix)]
        entries += [(n, False) for n in sorted(FILES) if n.startswith(prefix)]
        if query.get("recursive") != "true":
            entries = [(n, d) for n, d in entries if "/" not in n[len(prefix) :]]
        paths = []
        for name, is_dir in entries:
            entry = dict(
                name=name,
                lastModified=NOW,
                contentLength="0" if is_dir else str(len(FILES[name])),
                owner="$superuser",
                group="$superuser",
                permissions="rwxr-x---",
                etag="0x1",
            )
            if is_dir:
                entry["isDirectory"] = "true"
            paths.append(entry)
        self.reply(200, json.dumps({"paths": paths}).encode(), {"Content-Type": "application/json"})

    def list_blobs(self, query):
        prefix = query.get("prefix", "")
        blobs = "".join(
            f"<Blob><Name>{escape(n)}</Name><Properties><Last-Modified>{NOW}</Last-Modified><Etag>0x1</Etag>"
            f"<Content-Length>{len(FILES[n])}</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob>"
            for n in sorted(FILES)
            if n.startswith(prefix)
        )
        body = (
            f'<?xml version="1.0" encoding="utf-8"?><EnumerationResults ContainerName="c"><Prefix>{escape(prefix)}'
            f"</Prefix><Blobs>{blobs}</Blobs><NextMarker /></EnumerationResults>"
        )
        self.reply(200, body.encode(), {"Content-Type": "application/xml"})


AT = f"abfss://c@{ACCT}.dfs.core.windows.net/"
SLASH = f"abfss://{ACCT}.dfs.core.windows.net/c/"

# (pattern, the prefix every result must carry, the object names it matches)
GLOBS = []
for prefix in (AT, SLASH, f"abfs://c@{ACCT}.dfs.core.windows.net/", "abfss://c/"):
    GLOBS += [
        (prefix + "data/*.csv", prefix, ["data/a.csv"]),
        (prefix + "*.csv", prefix, ["top.csv"]),
        (prefix + "data/**", prefix, ["data/a.csv", "data/sub/b.csv", "data/sub/deep/c.csv"]),
        (prefix + "data/*/*.csv", prefix, ["data/sub/b.csv"]),
        (prefix + "data/*/", prefix, []),
        (prefix + "sp ace/*.csv", prefix, ["sp ace/d.csv"]),
        (prefix + "pct%20dir/*.csv", prefix, ["pct%20dir/e.csv"]),
        (prefix + "uni/\u00fcn\u00ef/*.csv", prefix, ["uni/\u00fcn\u00ef/f.csv"]),
    ]
# the blob file system is not touched by #211; its spellings must stay as they are
GLOBS += [
    ("az://c/data/*.csv", "az://c/", ["data/a.csv"]),
    (
        f"az://{ACCT}.blob.core.windows.net/c/data/**",
        f"az://{ACCT}.blob.core.windows.net/c/",
        ["data/a.csv", "data/sub/b.csv", "data/sub/deep/c.csv"],
    ),
]

SETUP = """.output /dev/null
LOAD '{extension}';
SET azure_transport_option_type = 'curl';
CREATE SECRET s (TYPE azure, CONNECTION_STRING '{connection_string}');
.output
"""


def literal(s):
    return "'" + s.replace("'", "''") + "'"


def duckdb(shell, extension, connection_string, sql):
    setup = SETUP.format(extension=extension, connection_string=connection_string)
    proc = subprocess.run(
        [shell, "-unsigned", "-csv", "-noheader", "-bail"],
        input=setup + sql,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        return "ERROR " + proc.stderr.strip().splitlines()[0]
    return proc.stdout.strip()


def main():
    shell, extension = sys.argv[1:3]
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Mock)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    endpoint = f"http://127.0.0.1:{server.server_address[1]}/{ACCT}"
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName={ACCT};AccountKey=a2V5;"
        f"BlobEndpoint={endpoint};DfsEndpoint={endpoint}"
    )

    checks = []
    for pattern, prefix, names in GLOBS:
        sql = f"SELECT file FROM glob({literal(pattern)}) ORDER BY file;\n"
        checks.append((f"glob {pattern}", sql, "\n".join(prefix + n for n in names)))
    # a pattern without a wildcard is returned as is
    checks.append(("glob of a file", f"SELECT file FROM glob({literal(AT + 'data/a.csv')});\n", AT + "data/a.csv"))
    # DuckLake's orphan cleanup: paths built from data_path must equal what a glob over data_path returns
    for data_path in (AT + "lake/", SLASH + "lake/"):
        known = ", ".join(literal(data_path + f"main/t/ducklake-000{i}.parquet") for i in (1, 2))
        sql = f"SELECT count(*) FILTER (file NOT IN ({known})), count(*) FROM glob({literal(data_path + '**')});\n"
        checks.append((f"no orphans under {data_path}", sql, "0,2"))
    # the returned paths open
    checks.append(
        ("read_csv over a @ glob", f"SELECT sum(x), count(*) FROM read_csv({literal(AT + 'data/**')});\n", "6,3")
    )

    failures = 0
    for name, sql, want in checks:
        got = duckdb(shell, extension, connection_string, sql)
        got = "\n".join(line.strip('"') for line in got.splitlines())
        if got != want:
            failures += 1
            print(f"FAIL {name}\n  want: {want!r}\n  got:  {got!r}")
        else:
            print(f"ok   {name}")
    server.shutdown()
    print(f"{len(checks) - failures}/{len(checks)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
