// Consolidated integration tests for nativelink-store.
// Standalone test binaries are included as modules to cut per-binary link
// cost (notably Windows). CARVED OUT (kept as own binary, do NOT merge):
//   filesystem_store_test  - uses process-global fs::get_open_files_for_test()
//                            which is non-zero when sharing a process with
//                            other store tests running concurrently.
//   mongo_store_test       - local helper module (mongo_runner).
//   tls_client_no_roots_test - sets process-wide SSL_CERT_* env.

#[path = "ac_utils_test.rs"]
mod ac_utils_test;
#[path = "azure_blob_store_test.rs"]
mod azure_blob_store_test;
#[path = "cache_metrics_store_test.rs"]
mod cache_metrics_store_test;
#[path = "cas_utils_test.rs"]
mod cas_utils_test;
#[path = "common_s3_utils_test.rs"]
mod common_s3_utils_test;
#[path = "completeness_checking_store_test.rs"]
mod completeness_checking_store_test;
#[path = "compression_store_test.rs"]
mod compression_store_test;
#[path = "dedup_store_test.rs"]
mod dedup_store_test;
#[path = "existence_store_test.rs"]
mod existence_store_test;
#[path = "fast_slow_store_test.rs"]
mod fast_slow_store_test;
#[path = "gcs_client_test.rs"]
mod gcs_client_test;
#[path = "gcs_store_test.rs"]
mod gcs_store_test;
#[path = "grpc_read_batching_test.rs"]
mod grpc_read_batching_test;
#[path = "grpc_store_test.rs"]
mod grpc_store_test;
#[path = "memory_store_test.rs"]
mod memory_store_test;
#[path = "oci_store_test.rs"]
mod oci_store_test;
#[path = "ontap_s3_existence_cache_store_test.rs"]
mod ontap_s3_existence_cache_store_test;
#[path = "ontap_s3_store_test.rs"]
mod ontap_s3_store_test;
#[path = "r2_store_test.rs"]
mod r2_store_test;
#[path = "redis_store_test.rs"]
mod redis_store_test;
#[path = "ref_store_test.rs"]
mod ref_store_test;
#[path = "s3_store_test.rs"]
mod s3_store_test;
#[path = "shard_store_test.rs"]
mod shard_store_test;
#[path = "size_partitioning_store_test.rs"]
mod size_partitioning_store_test;
#[path = "store_manager_test.rs"]
mod store_manager_test;
#[path = "verify_store_test.rs"]
mod verify_store_test;
