#!/usr/bin/env python3
"""Builds a small, deterministic, structurally real OCI image layout.

One gzipped-tar layer, a config, a manifest, and an index — enough for
skopeo/crane to treat it as a genuine image, small enough for a sandboxed
check. Used by the oci-client-roundtrip and oci-projection checks.
"""

import gzip
import hashlib
import io
import json
import os
import sys
import tarfile


def main() -> None:
    base = sys.argv[1]
    blobs = os.path.join(base, "blobs", "sha256")
    os.makedirs(blobs, exist_ok=True)

    def put(data: bytes) -> tuple[str, int]:
        digest = hashlib.sha256(data).hexdigest()
        with open(os.path.join(blobs, digest), "wb") as f:
            f.write(data)
        return digest, len(data)

    tario = io.BytesIO()
    with tarfile.open(fileobj=tario, mode="w") as tar:
        payload = b"oci-registry-over-cas roundtrip payload\n" * 1000
        info = tarfile.TarInfo("etc/roundtrip.txt")
        info.size = len(payload)
        info.mtime = 0
        tar.addfile(info, io.BytesIO(payload))
    layer_tar = tario.getvalue()
    layer_gz = gzip.compress(layer_tar, mtime=0)
    diff_id = "sha256:" + hashlib.sha256(layer_tar).hexdigest()
    layer_digest, layer_size = put(layer_gz)

    config = json.dumps(
        {
            "architecture": "amd64",
            "os": "linux",
            "config": {},
            "rootfs": {"type": "layers", "diff_ids": [diff_id]},
        }
    ).encode()
    config_digest, config_size = put(config)

    manifest = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": f"sha256:{config_digest}",
                "size": config_size,
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": f"sha256:{layer_digest}",
                    "size": layer_size,
                }
            ],
        }
    ).encode()
    manifest_digest, manifest_size = put(manifest)

    index = {
        "schemaVersion": 2,
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": f"sha256:{manifest_digest}",
                "size": manifest_size,
                "annotations": {"org.opencontainers.image.ref.name": "v1"},
            }
        ],
    }
    with open(os.path.join(base, "index.json"), "w") as f:
        json.dump(index, f)
    with open(os.path.join(base, "oci-layout"), "w") as f:
        f.write('{"imageLayoutVersion": "1.0.0"}')
    print(f"sha256:{manifest_digest}")


if __name__ == "__main__":
    main()
