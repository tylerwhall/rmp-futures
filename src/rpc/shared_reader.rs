use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::channel::oneshot::{channel, Receiver, Sender};
use futures::io::Result as IoResult;
use futures::prelude::*;

use super::decode::{RpcMessage, RpcRequestFuture, RpcStream};
use super::MsgId;
use crate::decode::ValueFuture;

use slab::Slab;
use spin::Mutex as SpinMutex;

type ResponseSender<R> = Sender<SentResult<R>>;
type ResponseReceiver<R> = Receiver<SentResult<R>>;

/// Tracks outstanding requests.
///
/// The position in the slab is the message id, allowing O(1) lookup and
/// guaranteeing they are unique. The item in the slab is a channel to send
/// ownership of the reader to the sender of the request. It also contains a
/// channel to send ownership back when the response has been read.
struct Requests<R>(SpinMutex<Slab<ResponseSender<R>>>);

impl<R> Requests<R> {
    /// Allocate a new pending request. Returns the request id and a future to
    /// receive the response.
    fn new_request(&self) -> (MsgId, ResponseReceiver<R>) {
        let (sender, receiver) = channel();
        let key = self.0.lock().insert(sender);
        // request ids are supposed to be 32-bit. On a 64-bit machine, there
        // could technically be an overflow, but only if 2^32 outstanding
        // requests already exist.
        let key = u32::try_from(key).expect("too many concurrent requests");
        (key.into(), receiver)
    }

    fn remove(&self, id: MsgId) -> Option<ResponseSender<R>> {
        let key = u32::from(id) as usize;
        let mut slab = self.0.lock();
        if slab.contains(key) {
            Some(slab.remove(key))
        } else {
            None
        }
    }
}

type SentValueFuture<R> = ValueFuture<RpcResultFuture<R>>;
/// What we send to the client waiting on a method result
type SentResult<R> = Result<SentValueFuture<R>, SentValueFuture<R>>;

struct RpcResultFutureInner<R> {
    /// Underlying reader to get the value
    result: super::decode::RpcResultFuture<RpcStream<R>>,
    /// Channel to give the reader back
    sender: Sender<super::decode::RpcResultFuture<RpcStream<R>>>,
}

struct RpcResultFuture<R>(Option<RpcResultFutureInner<R>>);

impl<R: AsyncRead + Unpin> RpcResultFuture<R> {
    fn new(
        result: super::decode::RpcResultFuture<RpcStream<R>>,
        sender: Sender<super::decode::RpcResultFuture<RpcStream<R>>>,
    ) -> Self {
        RpcResultFuture(Some(RpcResultFutureInner { result, sender }))
    }

    fn from_result(
        result: Result<
            ValueFuture<super::decode::RpcResultFuture<RpcStream<R>>>,
            ValueFuture<super::decode::RpcResultFuture<RpcStream<R>>>,
        >,
    ) -> (
        SentResult<R>,
        Receiver<super::decode::RpcResultFuture<RpcStream<R>>>,
    ) {
        // Channel for sending the reader back
        let (sender, receiver) = channel();
        (
            match result {
                Ok(result) => Ok(result.wrap(|r| RpcResultFuture::new(r, sender))),
                Err(result) => Err(result.wrap(|r| RpcResultFuture::new(r, sender))),
            },
            receiver,
        )
    }
}

impl<R> Drop for RpcResultFuture<R> {
    fn drop(&mut self) {
        if let Some(s) = self.0.take() {
            // This would only fail if the main rpc task is dead
            let _ = s.sender.send(s.result);
        } else {
            panic!("RpcResultFuture already dropped");
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for RpcResultFuture<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        if let Some(s) = self.as_mut().0.as_mut() {
            super::decode::RpcResultFuture::poll_read(Pin::new(&mut s.result), cx, buf)
        } else {
            panic!("RpcResultFuture already dropped");
        }
    }
}

/// Like `RpcMessage` but only unsolicited types. Responses are handled before
/// this point.
pub enum RpcIncomingMessage<R> {
    Request(RpcRequestFuture<R>),
    Notify,
}

enum ReaderState<R> {
    Owned(RpcStream<R>),
    Busy,
}

impl<R> ReaderState<R> {
    fn take(&mut self) -> Self {
        std::mem::replace(self, ReaderState::Busy)
    }
}

pub struct SharedReader<R> {
    requests: Arc<Requests<R>>,
    state: ReaderState<R>,
}

impl<R: AsyncRead + Unpin> SharedReader<R> {
    pub async fn next<Fut: Future<Output = IoResult<RpcStream<R>>>>(
        &mut self,
        mut f: impl FnMut(RpcIncomingMessage<RpcStream<R>>) -> Fut,
    ) -> IoResult<R> {
        loop {
            self.state = match self.state.take() {
                ReaderState::Owned(stream) => {
                    match stream.next().await? {
                        RpcMessage::Request(req) => {
                            ReaderState::Owned(f(RpcIncomingMessage::Request(req)).await?)
                        }
                        RpcMessage::Notify => {
                            ReaderState::Owned(f(RpcIncomingMessage::Notify).await?)
                        }
                        RpcMessage::Response(rsp) => {
                            let id = rsp.id();
                            if let Some(sender) = self.requests.remove(id) {
                                // Decode the message to get an Ok/Err result
                                let result = rsp.result().await?;
                                let (result, receiver) = RpcResultFuture::from_result(result);
                                if let Err(_r) = sender.send(result) {
                                    println!("Got unsolicitied response {:?} (receiver dead)", id);
                                    // If the receiver was dropped, we get the
                                    // result back. Dropping it here will
                                    // complete our receiver just as if the
                                    // client code received it and dropped it.
                                }
                                // oneshot::Canceled should not be possible
                                // because drop on RpcResultFuture always sends
                                // to the sender before the sender is dropped.
                                let result = receiver.await.expect("reader not returned");
                                // Consume the rest of the message if the client did not
                                let stream = result.finish().await?;
                                ReaderState::Owned(stream)
                            } else {
                                // TODO: error! from log crate
                                println!("Got unsolicitied response {:?}", id);
                                // Consume this message and loop
                                let reader = rsp.skip().await?;
                                ReaderState::Owned(reader)
                            }
                        }
                    }
                }
                ReaderState::Busy => unreachable!(),
            }
        }
    }
}
