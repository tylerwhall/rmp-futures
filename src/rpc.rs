use std::convert::TryFrom;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::io::Error as IoError;
use futures::io::ErrorKind;
use futures::io::Result as IoResult;
use futures::prelude::*;

use crate::decode::{ArrayFuture, MsgPackFuture, StringFuture, ValueFuture};

pub enum RpcMessage<R> {
    Request(RpcRequestFuture<R>),
    Response(RpcResponseFuture<R>),
    Notify,
}

impl<R> RpcMessage<R> {
    fn request(array: ArrayFuture<R>, id: u32) -> Self {
        RpcMessage::Request(RpcRequestFuture { array, id })
    }

    fn response(array: ArrayFuture<R>, id: u32) -> Self {
        RpcMessage::Response(RpcResponseFuture { array, id })
    }
}

pub struct RpcRequestFuture<R> {
    array: ArrayFuture<R>,
    id: u32,
}

impl<R: AsyncRead + Unpin> RpcRequestFuture<R> {
    pub async fn method(self) -> IoResult<StringFuture<RpcParamsFuture<R>>> {
        self.array
            .next()
            .into_option()
            // Wrap with RpcParamsFuture before potentially returning the ValueFuture
            .map(|m| MsgPackFuture::new(RpcParamsFuture(m.into_inner())))
            .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "array missing method field"))?
            .decode()
            .await?
            .into_string()
            .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "expected method string"))
    }
}

pub struct RpcParamsFuture<R>(ArrayFuture<R>);

impl<R: AsyncRead + Unpin> RpcParamsFuture<R> {
    pub async fn params(self) -> IoResult<ArrayFuture<R>> {
        self.0
            .last()
            .into_option()
            .ok_or_else(|| {
                IoError::new(
                    ErrorKind::InvalidData,
                    "array missing params or too many fields",
                )
            })?
            .decode()
            .await?
            .into_array()
            .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "expected params array"))
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for RpcParamsFuture<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        ArrayFuture::poll_read(Pin::new(&mut self.as_mut().0), cx, buf)
    }
}

pub struct RpcResponseFuture<R> {
    array: ArrayFuture<R>,
    id: u32,
}

impl<R: AsyncRead + Unpin> RpcResponseFuture<R> {
    pub fn id(&self) -> u32 {
        self.id
    }

    pub async fn result(
        self,
    ) -> IoResult<Result<ValueFuture<RpcResultFuture<R>>, ValueFuture<RpcResultFuture<R>>>> {
        let err = self
            .array
            .next()
            .into_option()
            // Wrap with RpcResultFuture before potentially returning the ValueFuture
            .map(|m| MsgPackFuture::new(RpcResultFuture(m.into_inner())))
            .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "array missing error field"))?
            .decode()
            .await?;
        if let ValueFuture::Nil(m) = err {
            m.0.next()
                .into_option()
                // Wrap with RpcResultFuture before potentially returning the ValueFuture
                .map(|m| MsgPackFuture::new(RpcResultFuture(m.into_inner())))
                .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "array missing result field"))?
                .decode()
                .await
                .map(Err)
        } else {
            Ok(Err(err))
        }
    }
}

/// Container that ensures the response message array is consumed before
/// returning the underlying reader
pub struct RpcResultFuture<R>(ArrayFuture<R>);

impl<R: AsyncRead + Unpin> RpcResultFuture<R> {
    pub async fn finish(self) -> IoResult<R> {
        self.0.skip().await
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for RpcResultFuture<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        ArrayFuture::poll_read(Pin::new(&mut self.as_mut().0), cx, buf)
    }
}

pub struct RpcStream<R> {
    msg: MsgPackFuture<R>,
}

impl<R: AsyncRead + Unpin> RpcStream<R> {
    pub async fn next(self) -> IoResult<RpcMessage<R>> {
        let a = self
            .msg
            .decode()
            .await?
            .into_array()
            .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "expected array"))?;
        let ty = a
            .next()
            .into_option()
            .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "msgpack array 0-length"))?;
        let (ty, array) = ty
            .decode()
            .await?
            .into_u64()
            .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "msgtype not int"))?;
        match ty {
            0 | 1 => {
                // Request or Response
                let msgid = array.next().into_option().ok_or_else(|| {
                    IoError::new(ErrorKind::InvalidData, "msgpack array 0-length")
                })?;
                let (msgid, array) = msgid
                    .decode()
                    .await?
                    .into_u64()
                    .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "msgid not int"))?;
                let msgid = u32::try_from(msgid)
                    .map_err(|_| IoError::new(ErrorKind::InvalidData, "msgid out of range"))?;
                match ty {
                    0 => Ok(RpcMessage::request(array, msgid)),
                    _ => Ok(RpcMessage::response(array, msgid)),
                }
            }
            2 => {
                // Notify
                unimplemented!()
            }
            ty => Err(IoError::new(
                ErrorKind::InvalidData,
                format!("invalid msgtype {}", ty),
            )),
        }
    }
}
