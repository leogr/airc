#!/usr/bin/env python3
"""Private retained-job bookkeeping. Never starts, signals, or observes a process."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import secrets
import stat
import sys

LIMIT = 16384
FIELDS = {"version", "session", "handle", "mode", "wake", "airc", "state_dir", "thread", "skill", "phase", "updated", "outcome"}


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def checked(fd, directory=False):
    info = os.fstat(fd)
    valid_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if not valid_type or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("monitor state must be private and owned by the current user")
    if not directory and info.st_nlink != 1:
        raise ValueError("monitor state must have exactly one link")


@contextmanager
def locked(path):
    path = Path(path)
    if not path.is_absolute() or path.name in ("", ".", "..") or ".." in path.parts:
        raise ValueError("--file requires an absolute path without parent traversal")
    # Walk by descriptor: reject symlinks in every component, including ancestors.
    walk_flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW
    directory = os.open("/", walk_flags)
    lock = None
    try:
        for part in path.parent.parts[1:]:
            child = os.open(part, walk_flags, dir_fd=directory)
            os.close(directory)
            directory = child
        parent = os.open(".", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
        os.close(directory)
        directory = parent
        checked(directory, directory=True)
        lock = os.open(path.name + ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=directory)
        checked(lock)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield directory, path.name
    finally:
        if lock is not None:
            os.close(lock)
        os.close(directory)


def read(directory, name):
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as source:
        checked(source.fileno())
        value = source.read(LIMIT + 1)
    if len(value) > LIMIT:
        raise ValueError("monitor record exceeds its size limit")
    record = json.loads(value)
    if not isinstance(record, dict) or set(record) != FIELDS or record["version"] != 1:
        raise ValueError("unsupported or malformed monitor record")
    for field in FIELDS - {"version"}:
        value = record[field]
        if not isinstance(value, str) or len(value) > 4096 or any(ord(c) < 32 for c in value):
            raise ValueError("invalid monitor field")
    if record["phase"] not in ("retained", "handling") or bool(record["handle"]) != (record["phase"] == "retained"):
        raise ValueError("inconsistent retained handle")
    return record


def save(directory, name, record):
    value = (json.dumps(record, sort_keys=True) + "\n").encode()
    if len(value) > LIMIT:
        raise ValueError("monitor record exceeds its size limit")
    temp = ".monitor-" + secrets.token_hex(12)
    try:
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        with os.fdopen(fd, "wb") as dest:
            dest.write(value)
            dest.flush()
            os.fsync(dest.fileno())
        os.rename(temp, name, src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
    finally:
        try:
            os.unlink(temp, dir_fd=directory)
        except FileNotFoundError:
            pass


def text(value):
    if not value or len(value) > 4096 or any(ord(c) < 32 for c in value):
        raise argparse.ArgumentTypeError("expected bounded nonempty text without control characters")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, help="absolute JSON path in an existing private directory")
    commands = parser.add_subparsers(dest="command", required=True)
    record = commands.add_parser("record", help="retain exactly one harness job handle")
    for field in ("session", "handle", "airc", "skill"):
        record.add_argument("--" + field, required=True, type=text)
    record.add_argument("--mode", choices=("native", "codex-queue", "active-turn"), required=True)
    record.add_argument("--wake", choices=("all", "messages", "mentions"), default="messages")
    record.add_argument("--state-dir", type=text)
    record.add_argument("--thread", type=text)
    handling = commands.add_parser("handling", help="record an observed terminal job outcome; does not cancel anything")
    handling.add_argument("--handle", type=text, required=True)
    handling.add_argument("--outcome", choices=("completed", "cancelled", "failed"), required=True)
    commands.add_parser("status", help="read bookkeeping only; use airc status for waiter evidence")
    args = parser.parse_args()
    with locked(args.file) as (directory, name):
        previous = read(directory, name)
        if args.command == "record":
            args.state_dir = args.state_dir or ""
            args.thread = args.thread or ""
            if args.mode == "codex-queue" and not args.thread:
                raise ValueError("codex-queue needs the known current thread ID")
            for field in ("airc", "skill", "state_dir"):
                value = getattr(args, field)
                if value and not Path(value).is_absolute():
                    raise ValueError(field + " must be an absolute path")
            value = {key: getattr(args, key) for key in ("session", "handle", "mode", "wake", "airc", "state_dir", "thread", "skill")}
            if previous and previous["handle"]:
                if any(previous[key] != item for key, item in value.items()):
                    raise ValueError("a retained handle already exists; inspect/reap it and record its outcome before replacing it")
                value = previous
            else:
                value.update(version=1, phase="retained", updated=now(), outcome="")
                save(directory, name, value)
        elif args.command == "handling":
            if not previous or previous["handle"] != args.handle:
                raise ValueError("handle does not match the retained job")
            value = dict(previous, handle="", phase="handling", outcome=args.outcome, updated=now())
            save(directory, name, value)
        else:
            value = previous
        print(json.dumps({"record": value, "evidence": "bookkeeping only; use airc status for worker-observed waiter; no future model wake is proven"}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        print("airc-monitor: " + str(error), file=sys.stderr)
        sys.exit(1)
