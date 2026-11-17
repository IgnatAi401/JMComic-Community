#!/usr/bin/env python3
"""Server-side admin commands. Runs against the SQLite file directly; the server may keep running."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from community_server import DEFAULT_DB, comic_key, open_db


def show(conn, hidden: bool) -> None:
    rows = conn.execute(
        "SELECT * FROM comics WHERE hidden = ? ORDER BY last_at DESC LIMIT 200", (int(hidden),)
    ).fetchall()
    if not rows:
        print("（空）")
    for row in rows:
        when = time.strftime("%Y-%m-%d %H:%M", time.localtime(row["last_at"]))
        print(f"{row['comic_id']:>10}  推荐 {row['recommenders']:>3}  好看 {row['good']:>3}  "
              f"不好看 {row['bad']:>3}  最近 {when}")


def main() -> None:
    parser = argparse.ArgumentParser(description="JMComic Community 管理")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="列出推荐（最近 200 条）")
    sub.add_parser("hidden", help="列出已隐藏的推荐")
    for name, text in (("hide", "从推荐页隐藏"), ("unhide", "恢复显示")):
        cmd = sub.add_parser(name, help=text)
        cmd.add_argument("comic", nargs="+", help="漫画 ID")
    args = parser.parse_args()

    conn = open_db(args.db)
    if args.command in {"list", "hidden"}:
        show(conn, args.command == "hidden")
        return
    flag = int(args.command == "hide")
    for raw in args.comic:
        comic = comic_key(raw)
        changed = conn.execute("UPDATE comics SET hidden = ? WHERE comic_id = ?", (flag, comic)).rowcount
        print(f"{comic}: {'已' + ('隐藏' if flag else '恢复') if changed else '不在推荐列表里'}")


if __name__ == "__main__":
    main()
