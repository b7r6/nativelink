// Diagnostic: project a local OCI layer and dump a Directory subtree's node
// digests, to localize a §7 projection divergence. Env-gated (inert in CI).
//   LAYER_GZ=<oci>/blobs/sha256/<gzip-blob> SUBPATH=toolchain/bin \
//   cargo test -p nativelink-oci --test dump_root -- --nocapture
use std::collections::HashMap;

use nativelink_oci::projection::{ProjectionDigestFunction, decompress_gzip, project_layers};
use nativelink_proto::build::bazel::remote::execution::v2::Directory as ProtoDirectory;
use prost::Message;

#[test]
fn dump_root() {
    let Ok(layer_gz) = std::env::var("LAYER_GZ") else {
        eprintln!("LAYER_GZ unset — skipping");
        return;
    };
    let gz = std::fs::read(&layer_gz).expect("read layer");
    let tar = decompress_gzip(&gz).expect("gunzip");
    let proj =
        project_layers(&[&tar], ProjectionDigestFunction::Blake3, None).expect("project_layers");

    // Index every Directory proto by its digest hash for navigation.
    let by_digest: HashMap<String, Vec<u8>> = proj
        .directory_blobs
        .iter()
        .map(|(d, b)| (d.hash.clone(), b.to_vec()))
        .collect();

    // Navigate SUBPATH (slash-separated) from the root by decoding child protos.
    let subpath = std::env::var("SUBPATH").unwrap_or_default();
    let mut cur = proj.root.clone();
    let mut cur_bytes = proj
        .directory_blobs
        .get(&proj.root_digest)
        .expect("root blob")
        .to_vec();
    for comp in subpath.split('/').filter(|c| !c.is_empty()) {
        let dn = cur
            .directories
            .iter()
            .find(|d| d.name == comp)
            .unwrap_or_else(|| panic!("no subdir '{comp}'"));
        cur_bytes = by_digest
            .get(&dn.digest.as_ref().unwrap().hash)
            .expect("dir blob present")
            .clone();
        cur = ProtoDirectory::decode(cur_bytes.as_slice()).expect("decode dir");
    }
    if let Ok(out) = std::env::var("DUMP_OUT") {
        std::fs::write(&out, &cur_bytes).unwrap();
        eprintln!("wrote {} bytes to {out}", cur_bytes.len());
    }

    eprintln!("consumer SUBPATH='{subpath}':");
    for f in &cur.files {
        let dg = f.digest.as_ref().unwrap();
        eprintln!(
            "  file {} -> {}/{} exec={}",
            f.name, dg.hash, dg.size_bytes, f.is_executable
        );
    }
    for d in &cur.directories {
        let dg = d.digest.as_ref().unwrap();
        eprintln!("  dir {} -> {}/{}", d.name, dg.hash, dg.size_bytes);
    }
    for s in &cur.symlinks {
        eprintln!("  sym {} -> {}", s.name, s.target);
    }
}
