use nativelink_error::{Code, Error};
use walkdir::WalkDir;

// A transport-layer failure (mid-stream connection reset) is reported by tonic
// as `Code::Unknown` "transport error", but it is transient and must reach the
// client as the retryable `Unavailable` — otherwise big cacheable uploads are
// silently dropped when a shard resets the connection mid-write.
#[test]
fn transport_unknown_status_maps_to_unavailable() {
    let status = tonic::Status::unknown("transport error");
    let err: Error = status.into();
    assert_eq!(err.code, Code::Unavailable);
}

// A genuinely-unmapped `Unknown` (no transport origin) is preserved as-is.
#[test]
fn genuine_unknown_status_is_preserved() {
    let status = tonic::Status::unknown("some application-level failure");
    let err: Error = status.into();
    assert_eq!(err.code, Code::Unknown);
}

#[test]
fn walkdir_source_error() {
    for entry in WalkDir::new("/bad/path") {
        let err: Error = entry.unwrap_err().into();
        let os_error = {
            #[cfg(unix)]
            {
                "No such file or directory (os error 2)"
            }
            #[cfg(windows)]
            {
                "The system cannot find the path specified. (os error 3)"
            }
        };
        assert_eq!(
            err.messages,
            vec![
                os_error,
                &format!("IO error for operation on /bad/path: {os_error}")
            ]
        );
    }
}
