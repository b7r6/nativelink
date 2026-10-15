"""Tests for verify_fast_store.py. Run: python3 -m unittest discover tools/provenance"""

import contextlib
import hashlib
import io
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import verify_fast_store  # noqa: E402


def put(directory, data, name=None, suffix=""):
    os.makedirs(directory, exist_ok=True)
    digest = hashlib.sha256(data).hexdigest()
    name = name or f"{digest}-{len(data)}{suffix}"
    with open(os.path.join(directory, name), "wb") as f:
        f.write(data)
    return name


class VerifyFastStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.content = os.path.join(self.tmp.name, "content_path-cas")
        self.d2 = os.path.join(self.content, "d2")
        self.exec_d2 = self.content + ".exec/d2"

    def tearDown(self):
        self.tmp.cleanup()

    def run_check(self, do_hash):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            bad = verify_fast_store.check([self.content], do_hash, "sha256")
        return bad, out.getvalue()

    def test_intact_store_passes(self):
        put(self.d2, b"hello", suffix="-10")
        put(self.exec_d2, b"tool")
        bad, out = self.run_check(do_hash=True)
        self.assertEqual(bad, 0)
        self.assertIn("checked 2 blobs", out)

    def test_appended_exec_variant_fails_on_size(self):
        name = put(self.exec_d2, b"input")
        with open(os.path.join(self.exec_d2, name), "ab") as f:
            f.write(b"scribbled\n")
        bad, out = self.run_check(do_hash=False)
        self.assertEqual(bad, 1)
        self.assertIn("size 15, name says 5", out)

    def test_same_size_rewrite_needs_hash(self):
        name = put(self.d2, b"aaaa")
        with open(os.path.join(self.d2, name), "wb") as f:
            f.write(b"bbbb")
        self.assertEqual(self.run_check(do_hash=False)[0], 0)
        bad, out = self.run_check(do_hash=True)
        self.assertEqual(bad, 1)
        self.assertIn("content hashes to", out)

    def test_ignores_files_that_are_not_digests(self):
        put(self.d2, b"x", name="not-a-digest")
        self.assertEqual(self.run_check(do_hash=True), (0, "checked 0 blobs (size and sha256 content); 0 mismatched\n"))


if __name__ == "__main__":
    unittest.main()
