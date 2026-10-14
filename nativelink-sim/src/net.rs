// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! tonic over turmoil's simulated TCP: a server's incoming connections from a
//! turmoil listener, and a client channel whose connector dials turmoil.

use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::io;

use futures::Stream;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::server::Connected;
use tonic::transport::{Channel, Endpoint, Uri};

/// The port every scheduler host serves gRPC on.
pub const SCHEDULER_PORT: u16 = 50051;

/// An accepted simulated connection, as tonic's server wants it.
#[derive(Debug)]
pub struct SimConn(pub turmoil::net::TcpStream);

impl Connected for SimConn {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl AsyncRead for SimConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for SimConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// Every connection accepted on `listener`, for `serve_with_incoming`.
pub fn incoming(
    listener: turmoil::net::TcpListener,
) -> impl Stream<Item = Result<SimConn, io::Error>> {
    futures::stream::unfold(listener, |listener| async move {
        let accepted = listener.accept().await.map(|(stream, _)| SimConn(stream));
        Some((accepted, listener))
    })
}

/// How a simulated client connection behaves.
#[derive(Clone, Copy, Debug)]
pub struct ChannelOptions {
    /// Give up on a connection attempt after this long.
    pub connect_timeout: Duration,
    /// HTTP/2 PING interval and timeout, so a silently dead connection (a
    /// black-holed link) is noticed. `None` disables keepalive.
    pub keepalive: Option<(Duration, Duration)>,
}

/// Connect to `host:port` over the simulated network.
pub async fn connect(
    host: &str,
    port: u16,
    options: ChannelOptions,
) -> Result<Channel, tonic::transport::Error> {
    let mut endpoint = Endpoint::from_shared(format!("http://{host}:{port}"))?
        .connect_timeout(options.connect_timeout);
    if let Some((interval, timeout)) = options.keepalive {
        endpoint = endpoint
            .http2_keep_alive_interval(interval)
            .keep_alive_timeout(timeout)
            .keep_alive_while_idle(true);
    }
    endpoint
        .connect_with_connector(tower::service_fn(|uri: Uri| async move {
            let host = uri
                .host()
                .ok_or_else(|| io::Error::other("uri without host"))?
                .to_string();
            let port = uri
                .port_u16()
                .ok_or_else(|| io::Error::other("uri without port"))?;
            let stream = turmoil::net::TcpStream::connect((host.as_str(), port)).await?;
            Ok::<_, io::Error>(TokioIo::new(stream))
        }))
        .await
}
