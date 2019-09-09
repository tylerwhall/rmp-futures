use std::pin::Pin;
use std::task::{Context, Poll};

use futures::io::Result as IoResult;
use futures::prelude::*;

use super::*;
use crate::encode::{ArrayFuture, MsgPackWriter};

pub struct RpcSink<W> {
    writer: W,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for RpcSink<W> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<IoResult<usize>> {
        W::poll_write(Pin::new(&mut self.as_mut().writer), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<IoResult<()>> {
        W::poll_flush(Pin::new(&mut self.as_mut().writer), cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<IoResult<()>> {
        W::poll_close(Pin::new(&mut self.as_mut().writer), cx)
    }
}

impl<W: AsyncWrite + Unpin> RpcSink<W> {
    pub fn new(writer: W) -> Self {
        RpcSink { writer }
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    pub async fn write_request(
        self,
        msgid: MsgId,
        method: impl AsRef<str>,
        num_args: u32,
    ) -> IoResult<ArrayFuture<RpcSink<W>>> {
        // First, wrap our RpcSink in a MsgPackWriter rather than using the
        // underlying writer. When this message is fully written and its writer
        // is returned, the client will be left with this RpcSink pointing at
        // the next message.
        let args = MsgPackWriter::new(self)
            .write_array_len(4)
            .await?
            .next()
            .unwrap()
            .write_int(MsgType::Request)
            .await?
            .next()
            .unwrap()
            .write_int(msgid)
            .await?
            .next()
            .unwrap()
            .write_str(method.as_ref())
            .await?
            .last()
            .unwrap();
        args.write_array_len(num_args).await
    }
}
