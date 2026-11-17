#!/usr/bin/env python3
"""JMComic Community: anonymous online counter and shared recommendation list.

Python standard library only. Every request is short; the server keeps the online
table in memory and everything else in one SQLite file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sqlite3
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_DB = ROOT_DIR / "data" / "community.sqlite3"

BASE_PATH = "/jm/api"
MAX_BODY_BYTES = 4 * 1024
ONLINE_TTL = 150          # seconds without a heartbeat before a client counts as offline
SAMPLE_INTERVAL = 60      # seconds between online samples for the hourly history
HOUR = 3600
DAY = 86400
DAY_OFFSET = 8 * HOUR     # calendar days are cut at midnight Beijing time (UTC+8, no DST)
# History ranges: bucket size and bucket count (None = since the first sample).
RANGES = {"day": (HOUR, 24), "week": (HOUR, 24 * 7), "month": (DAY, 30), "all": (DAY, None)}
MAX_ALL_DAYS = 366 * 10
PAGE_SIZE = 24
MAX_PAGE = 500
HOT_GRAVITY = 0.8

CLIENT_ID = re.compile(r"^[A-Za-z0-9_-]{16,64}$")
COMIC_ID = re.compile(r"^[0-9]{1,12}$")
VOTES = {"good": 1, "bad": -1, None: 0}
VOTE_NAMES = {1: "good", -1: "bad", 0: None}
# Emoji reactions, by stable key; the frontends map each key to its emoji.
REACTIONS = ("love", "hot", "lol", "mindblown", "think", "cry", "angry",
             "fire", "zap", "cow", "chicken", "milk", "nails", "zero")

SCHEMA = """
CREATE TABLE IF NOT EXISTS comics (
    comic_id     TEXT PRIMARY KEY,
    first_at     INTEGER NOT NULL,
    last_at      INTEGER NOT NULL,
    recommenders INTEGER NOT NULL DEFAULT 0,
    good         INTEGER NOT NULL DEFAULT 0,
    bad          INTEGER NOT NULL DEFAULT 0,
    hidden       INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS recommends (
    comic_id   TEXT NOT NULL,
    client     TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (comic_id, client)
);
CREATE TABLE IF NOT EXISTS votes (
    comic_id   TEXT NOT NULL,
    client     TEXT NOT NULL,
    vote       INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (comic_id, client)
);
CREATE TABLE IF NOT EXISTS reactions (
    comic_id   TEXT NOT NULL,
    client     TEXT NOT NULL,
    reaction   TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (comic_id, client, reaction)
);
CREATE TABLE IF NOT EXISTS online_hours (
    hour_start  INTEGER PRIMARY KEY,
    sample_sum  INTEGER NOT NULL DEFAULT 0,
    samples     INTEGER NOT NULL DEFAULT 0,
    peak        INTEGER NOT NULL DEFAULT 0,
    unique_ids  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS online_days (
    day_start   INTEGER PRIMARY KEY,
    visitors    INTEGER NOT NULL DEFAULT 0
);
-- Hashed IDs seen in the current hour and day only, so a restart keeps counting;
-- rows from earlier periods are deleted as soon as the period ends.
CREATE TABLE IF NOT EXISTS online_seen (
    scope   TEXT NOT NULL,
    start   INTEGER NOT NULL,
    client  TEXT NOT NULL,
    PRIMARY KEY (scope, start, client)
);
CREATE INDEX IF NOT EXISTS comics_last ON comics (hidden, last_at DESC);
CREATE INDEX IF NOT EXISTS comics_top ON comics (hidden, recommenders DESC, last_at DESC);
"""


class ApiError(Exception):
    def __init__(self, status: HTTPStatus, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def client_key(raw: object) -> str:
    """Only a hash of the browser's random ID is kept, never the ID itself."""
    if not isinstance(raw, str) or not CLIENT_ID.match(raw):
        raise ApiError(HTTPStatus.BAD_REQUEST, "invalid client")
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def comic_key(raw: object) -> str:
    value = str(raw) if isinstance(raw, int) and not isinstance(raw, bool) else raw
    if not isinstance(value, str) or not COMIC_ID.match(value):
        raise ApiError(HTTPStatus.BAD_REQUEST, "invalid comic")
    return value.lstrip("0") or "0"


def hour_start(now: float) -> int:
    return int(now // HOUR * HOUR)


def day_start(now: float) -> int:
    return int((now + DAY_OFFSET) // DAY * DAY - DAY_OFFSET)


PERIODS = {"hour": hour_start, "day": day_start}


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA)
    return conn


class Community:
    def __init__(self, db_path: Path):
        self.db = open_db(db_path)
        self.db_lock = threading.Lock()
        self.online: dict[str, float] = {}
        self.online_lock = threading.Lock()
        # Unique visitors per period: scope -> [period start, hashed IDs seen in it].
        self.seen: dict[str, list] = {scope: [0, set()] for scope in PERIODS}
        self._load_seen()

    # ---- online ----

    def heartbeat(self, client: str, leaving: bool) -> int:
        now = time.time()
        new: list[tuple] = []
        with self.online_lock:
            if leaving:
                self.online.pop(client, None)
            else:
                self.online[client] = now
                rolled = self._roll(now)
                for scope, (start, clients) in self.seen.items():
                    if client not in clients:
                        clients.add(client)
                        new.append((scope, start, client))
            count = self._count(now)
        if not leaving:
            self._save_seen(new, rolled)
        return count

    def online_count(self) -> int:
        with self.online_lock:
            return self._count(time.time())

    def _count(self, now: float) -> int:
        cutoff = now - ONLINE_TTL
        stale = [key for key, seen in self.online.items() if seen < cutoff]
        for key in stale:
            del self.online[key]
        return len(self.online)

    def _roll(self, now: float) -> bool:
        """Start a fresh visitor set for every period that has ended; True if any did."""
        rolled = False
        for scope, period in self.seen.items():
            start = PERIODS[scope](now)
            if period[0] != start:
                period[0], period[1] = start, set()
                rolled = True
        return rolled

    def _load_seen(self) -> None:
        now = time.time()
        self._roll(now)
        with self.db_lock:
            for scope, (start, clients) in self.seen.items():
                clients.update(row[0] for row in self.db.execute(
                    "SELECT client FROM online_seen WHERE scope = ? AND start = ?", (scope, start)
                ))
        self._save_seen([], True)

    def _save_seen(self, new: list[tuple], rolled: bool) -> None:
        if not new and not rolled:
            return
        with self.db_lock:
            if new:
                self.db.executemany("INSERT OR IGNORE INTO online_seen (scope, start, client) VALUES (?, ?, ?)", new)
            if rolled:
                now = time.time()
                for scope, start_of in PERIODS.items():
                    self.db.execute("DELETE FROM online_seen WHERE scope = ? AND start < ?", (scope, start_of(now)))

    def sample(self) -> None:
        """Add one online sample to the current hour and day (called once a minute)."""
        now = time.time()
        with self.online_lock:
            rolled = self._roll(now)
            count = self._count(now)
            (hour, hour_ids), (day, day_ids) = self.seen["hour"], self.seen["day"]
            hour_visitors, day_visitors = len(hour_ids), len(day_ids)
        self._save_seen([], rolled)
        with self.db_lock:
            self.db.execute(
                """INSERT INTO online_hours (hour_start, sample_sum, samples, peak, unique_ids)
                   VALUES (?, ?, 1, ?, ?)
                   ON CONFLICT(hour_start) DO UPDATE SET
                     sample_sum = sample_sum + excluded.sample_sum,
                     samples = samples + 1,
                     peak = MAX(peak, excluded.peak),
                     unique_ids = MAX(unique_ids, excluded.unique_ids)""",
                (hour, count, count, hour_visitors),
            )
            self.db.execute(
                """INSERT INTO online_days (day_start, visitors) VALUES (?, ?)
                   ON CONFLICT(day_start) DO UPDATE SET visitors = MAX(visitors, excluded.visitors)""",
                (day, day_visitors),
            )

    def today_visitors(self) -> int:
        with self.online_lock:
            self._roll(time.time())
            return len(self.seen["day"][1])

    def history(self, range_name: str) -> dict:
        """Evenly spaced buckets for the range; a bucket without samples has null values."""
        step, count = RANGES[range_name]
        now = time.time()
        end = hour_start(now) if step == HOUR else day_start(now)
        with self.db_lock:
            if count is None:
                first = self.db.execute("SELECT MIN(hour_start) FROM online_hours").fetchone()[0]
                start = max(day_start(first) if first is not None else end, end - (MAX_ALL_DAYS - 1) * DAY)
            else:
                start = end - (count - 1) * step
            if step == HOUR:
                rows = self.db.execute(
                    """SELECT hour_start AS t, sample_sum, samples, peak, unique_ids AS visitors
                       FROM online_hours WHERE hour_start >= ?""",
                    (start,),
                ).fetchall()
                buckets = {row["t"]: dict(row) for row in rows}
            else:
                rows = self.db.execute(
                    """SELECT (hour_start + :off) / 86400 * 86400 - :off AS t,
                              SUM(sample_sum) AS sample_sum, SUM(samples) AS samples, MAX(peak) AS peak
                       FROM online_hours WHERE hour_start >= :start GROUP BY t""",
                    {"off": DAY_OFFSET, "start": start},
                ).fetchall()
                buckets = {row["t"]: dict(row) for row in rows}
                for row in self.db.execute(
                    "SELECT day_start, visitors FROM online_days WHERE day_start >= ?", (start,)
                ):
                    buckets.setdefault(row["day_start"], {})["visitors"] = row["visitors"]
        points = []
        for t in range(start, end + 1, step):
            row = buckets.get(t, {})
            samples = row.get("samples")
            points.append({
                "t": t,
                "average": round(row["sample_sum"] / samples, 2) if samples else None,
                "peak": row.get("peak") if samples else None,
                "visitors": row.get("visitors"),
            })
        return {"range": range_name, "step": step, "points": points}

    # ---- recommendations ----

    def recommend(self, client: str, comic: str) -> dict:
        now = int(time.time())
        with self.db_lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                inserted = self.db.execute(
                    "INSERT OR IGNORE INTO recommends (comic_id, client, created_at) VALUES (?, ?, ?)",
                    (comic, client, now),
                ).rowcount
                if inserted:
                    self.db.execute(
                        """INSERT INTO comics (comic_id, first_at, last_at, recommenders)
                           VALUES (?, ?, ?, 1)
                           ON CONFLICT(comic_id) DO UPDATE SET
                             last_at = excluded.last_at, recommenders = recommenders + 1""",
                        (comic, now, now),
                    )
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK")
                raise
            return self._item(comic, client)

    def vote(self, client: str, comic: str, vote: int) -> dict:
        now = int(time.time())
        with self.db_lock:
            self._require(comic)
            self.db.execute("BEGIN IMMEDIATE")
            try:
                old = self.db.execute(
                    "SELECT vote FROM votes WHERE comic_id = ? AND client = ?", (comic, client)
                ).fetchone()
                old_vote = old["vote"] if old else 0
                self.db.execute(
                    """INSERT INTO votes (comic_id, client, vote, updated_at) VALUES (?, ?, ?, ?)
                       ON CONFLICT(comic_id, client) DO UPDATE SET
                         vote = excluded.vote, updated_at = excluded.updated_at""",
                    (comic, client, vote, now),
                )
                self.db.execute(
                    "UPDATE comics SET good = good + ?, bad = bad + ? WHERE comic_id = ?",
                    ((vote == 1) - (old_vote == 1), (vote == -1) - (old_vote == -1), comic),
                )
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK")
                raise
            return self._item(comic, client)

    def react(self, client: str, comic: str, reaction: str, on: bool) -> dict:
        with self.db_lock:
            self._require(comic)
            if on:
                self.db.execute(
                    "INSERT OR IGNORE INTO reactions (comic_id, client, reaction, created_at) VALUES (?, ?, ?, ?)",
                    (comic, client, reaction, int(time.time())),
                )
            else:
                self.db.execute(
                    "DELETE FROM reactions WHERE comic_id = ? AND client = ? AND reaction = ?", (comic, client, reaction)
                )
            return self._item(comic, client)

    def _require(self, comic: str) -> None:
        if not self.db.execute("SELECT 1 FROM comics WHERE comic_id = ? AND hidden = 0", (comic,)).fetchone():
            raise ApiError(HTTPStatus.NOT_FOUND, "not recommended")

    def comic(self, comic: str, client: str | None) -> dict:
        with self.db_lock:
            return self._item(comic, client)

    def listing(self, sort: str, page: int, client: str | None) -> dict:
        offset = (page - 1) * PAGE_SIZE
        with self.db_lock:
            total = self.db.execute("SELECT COUNT(*) FROM comics WHERE hidden = 0").fetchone()[0]
            if sort == "hot":
                # Small list: score in Python so the decay can use real exponents.
                rows = self.db.execute("SELECT * FROM comics WHERE hidden = 0").fetchall()
                now = time.time()
                rows.sort(key=lambda row: hot_score(row, now), reverse=True)
                rows = rows[offset:offset + PAGE_SIZE]
            else:
                order = "recommenders DESC, last_at DESC" if sort == "top" else "first_at DESC"
                rows = self.db.execute(
                    f"SELECT * FROM comics WHERE hidden = 0 ORDER BY {order} LIMIT ? OFFSET ?",
                    (PAGE_SIZE, offset),
                ).fetchall()
            ids = [row["comic_id"] for row in rows]
            mine = self._mine(ids, client)
            counts = self._reactions(ids)
        return {
            "items": [serialize(row, mine.get(row["comic_id"]), counts.get(row["comic_id"])) for row in rows],
            "total": total,
            "page": page,
            "pageSize": PAGE_SIZE,
        }

    def _item(self, comic: str, client: str | None) -> dict:
        row = self.db.execute("SELECT * FROM comics WHERE comic_id = ? AND hidden = 0", (comic,)).fetchone()
        if row is None:
            return {"id": comic, "recommenders": 0, "good": 0, "bad": 0, "reactions": {},
                    "firstAt": None, "lastAt": None, "mine": empty_mine()}
        return serialize(row, self._mine([comic], client).get(comic), self._reactions([comic]).get(comic))

    def _mine(self, comics: list[str], client: str | None) -> dict[str, dict]:
        if not client or not comics:
            return {}
        marks = ",".join("?" * len(comics))
        result = {comic: empty_mine() for comic in comics}
        for row in self.db.execute(
            f"SELECT comic_id FROM recommends WHERE client = ? AND comic_id IN ({marks})", (client, *comics)
        ):
            result[row["comic_id"]]["recommended"] = True
        for row in self.db.execute(
            f"SELECT comic_id, vote FROM votes WHERE client = ? AND comic_id IN ({marks})", (client, *comics)
        ):
            result[row["comic_id"]]["vote"] = VOTE_NAMES.get(row["vote"])
        for row in self.db.execute(
            f"SELECT comic_id, reaction FROM reactions WHERE client = ? AND comic_id IN ({marks})", (client, *comics)
        ):
            result[row["comic_id"]]["reactions"].append(row["reaction"])
        return result

    def _reactions(self, comics: list[str]) -> dict[str, dict[str, int]]:
        if not comics:
            return {}
        marks = ",".join("?" * len(comics))
        result: dict[str, dict[str, int]] = {}
        for row in self.db.execute(
            f"SELECT comic_id, reaction, COUNT(*) AS n FROM reactions WHERE comic_id IN ({marks}) GROUP BY comic_id, reaction",
            comics,
        ):
            result.setdefault(row["comic_id"], {})[row["reaction"]] = row["n"]
        return result


def hot_score(row: sqlite3.Row, now: float) -> float:
    points = row["recommenders"] * 3 + row["good"] - row["bad"]
    hours = max(0.0, (now - row["last_at"]) / 3600)
    return points / math.pow(hours + 2, HOT_GRAVITY)


def empty_mine() -> dict:
    return {"recommended": False, "vote": None, "reactions": []}


def serialize(row: sqlite3.Row, mine: dict | None, reactions: dict | None = None) -> dict:
    return {
        "id": row["comic_id"],
        "recommenders": row["recommenders"],
        "good": row["good"],
        "bad": row["bad"],
        "reactions": reactions or {},
        "firstAt": row["first_at"],
        "lastAt": row["last_at"],
        "mine": mine or empty_mine(),
    }


def make_handler(community: Community):
    class Handler(BaseHTTPRequestHandler):
        server_version = "JMComicCommunity/1.0"
        sys_version = ""
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):  # noqa: A002 - keep the log free of IPs and IDs
            pass

        def do_OPTIONS(self):
            self.send_response(HTTPStatus.NO_CONTENT)
            self._cors()
            self.send_header("Access-Control-Max-Age", "86400")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        def _dispatch(self, method: str):
            try:
                url = urlparse(self.path)
                if not url.path.startswith(BASE_PATH + "/"):
                    raise ApiError(HTTPStatus.NOT_FOUND, "not found")
                route = url.path[len(BASE_PATH):]
                query = {key: values[-1] for key, values in parse_qs(url.query).items()}
                body = self._body() if method == "POST" else {}
                self._send(HTTPStatus.OK, self._route(method, route, query, body))
            except ApiError as error:
                self._send(error.status, {"error": error.message})
            except Exception:  # noqa: BLE001 - never leak details to the browser
                self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "server error"})

        def _route(self, method: str, route: str, query: dict, body: dict):
            if route == "/health" and method == "GET":
                return {"ok": True}
            if route == "/heartbeat" and method == "POST":
                online = community.heartbeat(client_key(body.get("client")), bool(body.get("leaving")))
                return {"online": online}
            if route == "/online" and method == "GET":
                return {"online": community.online_count()}
            if route == "/stats" and method == "GET":
                range_name = query.get("range") or "week"
                if range_name not in RANGES:
                    raise ApiError(HTTPStatus.BAD_REQUEST, "invalid range")
                return {"online": community.online_count(), "today": community.today_visitors(),
                        **community.history(range_name)}
            if route == "/recommendations" and method == "GET":
                sort = query.get("sort", "hot")
                if sort not in {"hot", "new", "top"}:
                    raise ApiError(HTTPStatus.BAD_REQUEST, "invalid sort")
                page = int_arg(query.get("page"), 1, 1, MAX_PAGE)
                return community.listing(sort, page, optional_client(query.get("client")))
            if route.startswith("/comic/") and method == "GET":
                return community.comic(comic_key(route[len("/comic/"):]), optional_client(query.get("client")))
            if route == "/recommend" and method == "POST":
                return community.recommend(client_key(body.get("client")), comic_key(body.get("comic")))
            if route == "/vote" and method == "POST":
                vote = body.get("vote")
                if vote not in VOTES:
                    raise ApiError(HTTPStatus.BAD_REQUEST, "invalid vote")
                return community.vote(client_key(body.get("client")), comic_key(body.get("comic")), VOTES[vote])
            if route == "/react" and method == "POST":
                reaction = body.get("reaction")
                if reaction not in REACTIONS:
                    raise ApiError(HTTPStatus.BAD_REQUEST, "invalid reaction")
                return community.react(
                    client_key(body.get("client")), comic_key(body.get("comic")), reaction, body.get("on") is not False
                )
            raise ApiError(HTTPStatus.NOT_FOUND, "not found")

        def _body(self) -> dict:
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                raise ApiError(HTTPStatus.BAD_REQUEST, "invalid length") from None
            if length <= 0:
                return {}
            if length > MAX_BODY_BYTES:
                self.close_connection = True  # the unread body would corrupt the next request
                raise ApiError(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "body too large")
            # Any content type is accepted: browsers send text/plain to skip the CORS
            # preflight, and sendBeacon cannot set JSON headers anyway.
            try:
                data = json.loads(self.rfile.read(length))
            except (ValueError, UnicodeDecodeError):
                raise ApiError(HTTPStatus.BAD_REQUEST, "invalid json") from None
            if not isinstance(data, dict):
                raise ApiError(HTTPStatus.BAD_REQUEST, "invalid json")
            return data

        def _cors(self):
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")

        def _send(self, status: HTTPStatus, payload: dict):
            data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
            self.send_response(status)
            self._cors()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    return Handler


def int_arg(raw: str | None, default: int, low: int, high: int) -> int:
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ApiError(HTTPStatus.BAD_REQUEST, "invalid number") from None
    return min(high, max(low, value))


def optional_client(raw: str | None) -> str | None:
    return client_key(raw) if raw else None


def sampler(community: Community, stop: threading.Event) -> None:
    while not stop.wait(SAMPLE_INTERVAL - time.time() % SAMPLE_INTERVAL):
        try:
            community.sample()
        except Exception as error:  # noqa: BLE001 - keep sampling after a transient DB error
            print(f"sample failed: {error}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="JMComic Community server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8791)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    args = parser.parse_args()

    community = Community(args.db)
    stop = threading.Event()
    threading.Thread(target=sampler, args=(community, stop), daemon=True).start()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(community))
    server.daemon_threads = True
    print(f"JMComic Community: http://{args.host}:{args.port}{BASE_PATH}/health  (Ctrl+C 停止)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()


if __name__ == "__main__":
    main()
