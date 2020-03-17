use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

use futures::channel::oneshot::{channel, Receiver, Sender};
use futures::io::Result as IoResult;
use futures::prelude::*;

use super::decode::{RpcMessage, RpcNotifyFuture, RpcRequestFuture, RpcResponseFuture, RpcStream};
use super::MsgId;
use crate::decode::{ValueFuture, WrapReader};

use slab::Slab;

pub type ResponseSender<R> = Sender<SentResult<R>>;
pub type ResponseReceiver<R> = Receiver<SentResult<R>>;

/// Tracks outstanding requests.
///
/// The position in the slab is the message id, allowing O(1) lookup and
/// guaranteeing they are unique. The item in the slab is a channel to send
/// ownership of the reader to the sender of the request. It also contains a
/// channel to send ownership back when the response has been read.
pub struct RequestDispatch<R>(Mutex<Slab<ResponseSender<R>>>);

impl<R> Default for RequestDispatch<R> {
    fn default() -> Self {
        // Start with 0 capacity in the slab to use no memory if this is used as
        // a server only
        RequestDispatch(Mutex::new(Slab::new()))
    }
}

impl<R> RequestDispatch<R> {
    /// Allocate a new pending request. Returns the request id and a future to
    /// receive the response.
    pub fn new_request(&self) -> (MsgId, ResponseReceiver<R>) {
        let (sender, receiver) = channel();
        let key = self.0.lock().unwrap().insert(sender);
        // request ids are supposed to be 32-bit. On a 64-bit machine, there
        // could technically be an overflow, but only if 2^32 outstanding
        // requests already exist.
        let key = u32::try_from(key).expect("too many concurrent requests");
        (key.into(), receiver)
    }

    fn remove(&self, id: MsgId) -> Option<ResponseSender<R>> {
        let key = u32::from(id) as usize;
        let mut slab = self.0.lock().unwrap();
        if slab.contains(key) {
            Some(slab.remove(key))
        } else {
            None
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> RequestDispatch<R> {
    async fn dispatch_one(&self, rsp: RpcResponseFuture<RpcStream<R>>) -> IoResult<RpcStream<R>> {
        let id = rsp.id();
        if let Some(sender) = self.remove(id) {
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
            result.finish().await
        } else {
            // TODO: error! from log crate
            println!("Got unsolicitied response {:?}", id);
            // Consume this message and loop
            rsp.skip().await
        }
    }

    /// A future that dispatches method responses and never returns
    pub async fn dispatch(&self, mut stream: RpcStream<R>) -> IoResult<()> {
        loop {
            stream = match stream.next().await? {
                RpcMessage::Request(req) => req.skip().await?,
                RpcMessage::Notify(nfy) => nfy.skip().await?,
                RpcMessage::Response(rsp) => self.dispatch_one(rsp).await?,
            }
        }
    }

    /// Dispatches responses and yields requests and notifies
    pub async fn next(
        &self,
        mut stream: RpcStream<R>,
    ) -> IoResult<RpcIncomingMessage<RpcStream<R>>> {
        loop {
            stream = match stream.next().await? {
                RpcMessage::Request(req) => {
                    return Ok(RpcIncomingMessage::Request(req));
                }
                RpcMessage::Notify(nfy) => {
                    return Ok(RpcIncomingMessage::Notify(nfy));
                }
                RpcMessage::Response(rsp) => self.dispatch_one(rsp).await?,
            }
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

pub struct RpcResultFuture<R>(Option<RpcResultFutureInner<R>>);

pub type StreamResultFuture<R> = super::decode::RpcResultFuture<RpcStream<R>>;

impl<R: AsyncRead + Unpin> RpcResultFuture<R> {
    fn new(result: StreamResultFuture<R>, sender: Sender<StreamResultFuture<R>>) -> Self {
        RpcResultFuture(Some(RpcResultFutureInner { result, sender }))
    }

    fn from_result(
        result: Result<ValueFuture<StreamResultFuture<R>>, ValueFuture<StreamResultFuture<R>>>,
    ) -> (SentResult<R>, Receiver<StreamResultFuture<R>>) {
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
    Notify(RpcNotifyFuture<R>),
}
