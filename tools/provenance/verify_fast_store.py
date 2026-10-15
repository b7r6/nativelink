#!/usr/bin/env python3
"""Check a NativeLink filesystem store's blobs against the digests in their names.

Usage: verify_fast_store.py [--hash] [--algorithm sha256|blake3] CONTENT_PATH [...]

A filesystem store keeps each blob at `<content_path>/d2/<hash>-<size>[-<n>]`
and, for inputs a worker hardlinks as executable, a read-only copy at
`<content_path>.exec/d2/<hash>-<size>`. Workers hardlink these files straight
into action input trees, so an action that makes an input writable and changes
it in place changes the stored blob too. Every later action on that worker then
gets the changed bytes under the original digest, and its result is cached.

This walks both directories (the `.exec` sibling is found automatically) and
reports every file whose size, or with `--hash` whose content, does not match
its name. The size check reads no file data and takes seconds even on a large
store; `--hash` reads every byte. It only reads; it never changes the store.

Exit status: 0 if every checked file matches, 1 if any does not, 2 on bad usage.
"""

import argparse
import hashlib
import os
import re
import sys

NAME = re.compile(r"^([0-9a-f]{64})-(\d+)(?:-\d+)?$")


def hasher(algorithm):
    if algorithm == "sha256":
        return hashlib.sha256()
    if algorithm == "blake3":
        try:
            import blake3  # type: ignore[import-not-found]
        except ImportError:
            sys.exit("--algorithm blake3 needs the blake3 module (pip install blake3)")
        return blake3.blake3()
    sys.exit(f"unknown algorithm {algorithm}")


def file_digest(path, algorithm):
    h = hasher(algorithm)
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def directories(content_path):
    for base in (content_path, content_path.rstrip("/") + ".exec"):
        d = os.path.join(base, "d2")
        if os.path.isdir(d):
            yield d


def check(content_paths, do_hash, algorithm):
    checked = bad = 0
    for content_path in content_paths:
        for d in directories(content_path):
            for entry in os.scandir(d):
                m = NAME.match(entry.name)
                if not m or not entry.is_file(follow_symlinks=False):
                    continue
                want_hash, want_size = m.group(1), int(m.group(2))
                st = entry.stat(follow_symlinks=False)
                checked += 1
                problem = None
                if st.st_size != want_size:
                    problem = f"size {st.st_size}, name says {want_size}"
                elif do_hash:
                    got = file_digest(entry.path, algorithm)
                    if got != want_hash:
                        problem = f"content hashes to {got[:16]}…"
                if problem:
                    bad += 1
                    print(
                        f"MISMATCH {entry.path}: {problem}; mode {st.st_mode & 0o7777:o}, "
                        f"links {st.st_nlink}, modified {int(st.st_mtime)}, changed {int(st.st_ctime)}"
                    )
    mode = f"size and {algorithm} content" if do_hash else "size only"
    print(f"checked {checked} blobs ({mode}); {bad} mismatched")
    return bad


def main():
    parser = argparse.ArgumentParser(description="Check a NativeLink filesystem store's blobs against the digests in their names.")
    parser.add_argument("content_paths", nargs="+", metavar="CONTENT_PATH")
    parser.add_argument("--hash", action="store_true", help="also hash every blob (reads all data)")
    parser.add_argument("--algorithm", default="sha256", choices=["sha256", "blake3"])
    args = parser.parse_args()
    return 1 if check(args.content_paths, args.hash, args.algorithm) else 0


if __name__ == "__main__":
    sys.exit(main())
