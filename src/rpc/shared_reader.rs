use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

use futures::channel::oneshot::{channel, Receiver, Sender};
use futures::future::Either;
use futures::io::Result as IoResult;
use futures::prelude::*;

use super::decode::{RpcMessage, RpcNotifyFuture, RpcRequestFuture, RpcResponseFuture, RpcStream};
use super::encode::RpcSink;
use super::{MsgId, ResponseResult};
use crate::decode::{MarkerFuture, ValueFuture, WrapReader};
use crate::encode::ArrayFuture;

use slab::Slab;

pub type ResponseSender<R> = Sender<ResponseResult<R>>;
pub type ResponseReceiver<R> = Receiver<ResponseResult<R>>;

pub struct RequestDispatchInner<R> {
    requests: Slab<Option<ResponseSender<R>>>,
    shutdown_trigger: Option<Receiver<()>>,
    shutdown_waiter: Option<Sender<()>>,
}

/// Tracks outstanding requests.
///
/// The position in the slab is the message id, allowing O(1) lookup and
/// guaranteeing they are unique. The item in the slab is a channel to send
/// ownership of the reader to the sender of the request. It also contains a
/// channel to send ownership back when the response has been read.
pub struct RequestDispatch<R>(Mutex<RequestDispatchInner<R>>);

impl<R> Default for RequestDispatch<R> {
    fn default() -> Self {
        // Start with 0 capacity in the slab to use no memory if this is used as
        // a server only
        let (sender, receiver) = channel();
        RequestDispatch(Mutex::new(RequestDispatchInner {
            requests: Slab::new(),
            shutdown_trigger: Some(receiver),
            shutdown_waiter: Some(sender),
        }))
    }
}

impl<R> RequestDispatch<R> {
    /// Write a request and associate it with an id.
    ///
    /// Returns the writer for arguments and a future to receive the response.
    pub async fn write_request<W: AsyncWrite + Unpin>(
        &self,
        sink: RpcSink<W>,
        method: impl AsRef<str>,
        num_args: u32,
    ) -> (IoResult<ArrayFuture<RpcSink<W>>>, ResponseReceiver<R>) {
        let (sender, receiver) = channel();
        let writer = self
            ._write_request(sink, method, num_args, Some(sender))
            .await;
        (writer, receiver)
    }

    /// Write a request and associate it with an id. Corresponding response is ignored.
    ///
    /// Returns the writer for arguments
    pub async fn write_request_norsp<W: AsyncWrite + Unpin>(
        &self,
        sink: RpcSink<W>,
        method: impl AsRef<str>,
        num_args: u32,
    ) -> IoResult<ArrayFuture<RpcSink<W>>> {
        self._write_request(sink, method, num_args, None).await
    }

    async fn _write_request<W: AsyncWrite + Unpin>(
        &self,
        sink: RpcSink<W>,
        method: impl AsRef<str>,
        num_args: u32,
        sender: Option<ResponseSender<R>>,
    ) -> IoResult<ArrayFuture<RpcSink<W>>> {
        let key = self.0.lock().unwrap().requests.insert(sender);
        // request ids are supposed to be 32-bit. On a 64-bit machine, there
        // could technically be an overflow, but only if 2^32 outstanding
        // requests already exist.
        let key = u32::try_from(key).expect("too many concurrent requests");
        sink.write_request(key.into(), method, num_args).await
    }

    fn remove(&self, id: MsgId) -> Option<Option<ResponseSender<R>>> {
        let key = u32::from(id) as usize;
        let mut inner = self.0.lock().unwrap();
        if inner.requests.contains(key) {
            Some(inner.requests.remove(key))
        } else {
            None
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> RequestDispatch<R> {
    async fn dispatch_one(&self, rsp: RpcResponseFuture<RpcStream<R>>) -> IoResult<RpcStream<R>> {
        let id = rsp.id();
        match self.remove(id) {
            Some(Some(sender)) => {
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
            }
            Some(None) => {
                // Message exists, but nothing waiting on response. Drop it.
                rsp.skip().await
            }
            None => {
                // TODO: error! from log crate
                println!("Got unsolicitied response {:?}", id);
                // Consume this message and loop
                rsp.skip().await
            }
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

    /// Dispatch responses in a loop, yielding requests and notifies.
    ///
    /// Returns if `interrupt` is called, but only once there are no pending
    /// requests awaiting responses. The caller must take care to prevent
    /// sending more requests after interrupting if the goal is to quiesce.
    pub async fn dispatch_interruptible(
        &self,
        mut stream: RpcStream<R>,
    ) -> IoResult<RpcIteration<RpcStream<R>>> {
        let mut shutdown = if let Some(shutdown) = self.0.lock().unwrap().shutdown_waiter.take() {
            shutdown
        } else {
            // Exit immediately if we don't have the signal to wait on. That
            // means this is being called concurrently, which wouldn't make
            // sense anyway.
            return Ok(RpcIteration::None(stream));
        };

        let ret = loop {
            // Wait on the marker byte or cancellation
            let (marker, s) =
                match future::select(MarkerFuture::new(stream), shutdown.cancellation()).await {
                    Either::Left((marker_and_reader, _)) => marker_and_reader,
                    Either::Right((_, marker)) => {
                        // Cancel signaled. Exit if no pending method calls, else keep waiting on messages.
                        {
                            // New block to drop lock before await
                            let mut inner = self.0.lock().unwrap();
                            if inner.requests.is_empty() {
                                // Re-arm by creating a new sender/receiver pair
                                let (sender, receiver) = channel();
                                inner.shutdown_trigger = Some(receiver);
                                shutdown = sender;
                                break RpcIteration::None(marker.into_inner());
                            }
                        }
                        // Not ready to cancel. Continue waiting on marker.
                        marker.await
                    }
                }?;
            // Continue decoding after the marker byte
            let next = s.next_after_marker(marker).await?;

            stream = match next {
                RpcMessage::Request(req) => {
                    break RpcIteration::Some(RpcIncomingMessage::Request(req))
                }
                RpcMessage::Notify(nfy) => {
                    break RpcIteration::Some(RpcIncomingMessage::Notify(nfy))
                }
                RpcMessage::Response(rsp) => self.dispatch_one(rsp).await?,
            }
        };

        // Put the waiter back
        self.0.lock().unwrap().shutdown_waiter = Some(shutdown);

        Ok(ret)
    }

    /// Cause `dispatch_interruptible` to return on the next message boundary
    /// when there are no pending requests.
    pub fn interrupt(&self) {
        self.0.lock().unwrap().shutdown_trigger = None;
    }

    /// Processes one incoming message
    ///
    /// For responses, internally dispatch and return the reader
    /// For request/notify, return the `RpcIncomingMessage`
    pub async fn turn(&self, stream: RpcStream<R>) -> IoResult<RpcIteration<RpcStream<R>>> {
        match stream.next().await? {
            RpcMessage::Request(req) => Ok(RpcIteration::Some(RpcIncomingMessage::Request(req))),
            RpcMessage::Notify(nfy) => Ok(RpcIteration::Some(RpcIncomingMessage::Notify(nfy))),
            RpcMessage::Response(rsp) => Ok(RpcIteration::None(self.dispatch_one(rsp).await?)),
        }
    }

    /// Dispatches responses and yields requests and notifies
    ///
    /// Like `turn()` but only returns after reading a request or notify
    pub async fn next(
        &self,
        mut stream: RpcStream<R>,
    ) -> IoResult<RpcIncomingMessage<RpcStream<R>>> {
        loop {
            stream = match self.turn(stream).await? {
                RpcIteration::Some(ret) => return Ok(ret),
                RpcIteration::None(stream) => stream,
            }
        }
    }
}

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
    ) -> (ResponseResult<R>, Receiver<StreamResultFuture<R>>) {
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

/// For one iteration of `turn()`, represents an `RpcIncomingMessage` (request,
/// notify) or a response that was dispatched internally.
pub enum RpcIteration<R> {
    Some(RpcIncomingMessage<R>),
    None(R),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn dispatch() {
        use std::sync::Arc;

        use async_std::os::unix::net::UnixStream;
        use async_std::task;
        use rmpv::Value;

        let (client, mut server) = UnixStream::pair().unwrap();
        let (client_rx, client_tx) = client.split();
        let client_tx = RpcSink::new(client_tx);

        let dispatch = Arc::new(RequestDispatch::default());
        let dispatch2 = dispatch.clone();

        let dispatch_task = task::spawn(async move {
            dispatch2
                .dispatch_interruptible(RpcStream::new(client_rx))
                .await
        });

        // Task that waits for 2 messages, then replies to both
        let method_handler_task = task::spawn(async move {
            let mut ids = Vec::new();
            loop {
                match RpcStream::new(server).next().await.unwrap() {
                    RpcMessage::Notify(..) => unreachable!(),
                    RpcMessage::Request(req) => {
                        // Save the request id
                        ids.push(req.id());
                        // Consume the request
                        server = req.skip().await.unwrap().into_inner();
                        if ids.len() == 2 {
                            let mut sink = RpcSink::new(server);
                            for id in ids.drain(..) {
                                sink = sink
                                    .write_value_response(id, Ok(&Value::Nil))
                                    .await
                                    .unwrap();
                            }
                            break sink.into_inner();
                        }
                    }
                    RpcMessage::Response(..) => unreachable!(),
                }
            }
        });

        let (ret, rsp1) = dispatch.write_request(client_tx, "asdf", 0).await;
        let client_tx = ret.unwrap().end();
        let (ret, rsp2) = dispatch.write_request(client_tx, "asdf", 0).await;
        let _client_tx = ret.unwrap().end();

        match rsp1.await.unwrap() {
            Ok(ret) => assert_eq!(ret.into_value().await.unwrap().0, Value::Nil),
            Err(_) => panic!(),
        };
        // Interrupt while a call is pending
        dispatch.interrupt();
        // Dispatch should not be finished, so receiving second reply will work
        match rsp2.await.unwrap() {
            Ok(ret) => assert_eq!(ret.into_value().await.unwrap().0, Value::Nil),
            Err(_) => panic!(),
        };
        // Method handler should exit after 2 calls.
        let _server = method_handler_task.await;
        // Dispatch should exit now that all calls are finished
        dispatch_task.await.unwrap();
    }
}
