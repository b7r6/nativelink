fn main() {
    println!("cargo:rerun-if-changed=include/nativelink-keeper.h");
    if std::env::var("CARGO_FEATURE_EMBEDDED").is_err() {
        return;
    }
    let bindings = bindgen::Builder::default()
        .header("include/nativelink-keeper.h")
        .allowlist_function("nlk_.*")
        .allowlist_type("nlk_.*")
        .generate()
        .expect("bindgen");
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    bindings.write_to_file(out.join("bindings.rs")).expect("write bindings");
    // Link lines are emitted by the shim build (see shim/CMakeLists.txt,
    // driven from ci or a `just keeper` target); the recon report fills in
    // the static-lib names here.
    println!("cargo:rustc-link-search=native={}", std::env::var("NLK_LIB_DIR").unwrap_or_else(|_| "nativelink-keeper/shim/build".into()));
    println!("cargo:rustc-link-lib=static=nativelink_keeper_shim");
}
