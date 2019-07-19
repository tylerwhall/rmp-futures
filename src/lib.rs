#![feature(async_await)]

use std::pin::Pin;
use std::task::{Context, Poll};

use rmp::Marker;
use rmpv::Integer;

use byteorder::{BigEndian, ByteOrder};
use futures::io::Result as IoResult;
use futures::prelude::*;

#[derive(Debug)]
pub enum ValueFuture<R> {
    Nil,
    Boolean(bool, R),
    Integer(Integer, R),
    Array(ArrayFuture<R>),
}

impl<R> ValueFuture<R> {
    pub fn as_bool(self) -> Option<(bool, R)> {
        if let ValueFuture::Boolean(val, r) = self {
            Some((val, r))
        } else {
            None
        }
    }

    pub fn as_array(self) -> Option<ArrayFuture<R>> {
        if let ValueFuture::Array(array) = self {
            Some(array)
        } else {
            None
        }
    }

    pub fn as_u64(self) -> Option<(u64, R)> {
        if let ValueFuture::Integer(val, r) = self {
            val.as_u64().map(|val| (val, r))
        } else {
            None
        }
    }
}

/// Used when iterating over collections, to return either the next item or
/// indicate end of the collection, returning the underlying reader.
pub enum MsgPackOption<T, U> {
    Some(T),
    End(U),
}

pub struct MsgPackFuture<R> {
    reader: R,
}

impl<R: AsyncRead + Unpin> MsgPackFuture<R> {
    fn new(reader: R) -> Self {
        MsgPackFuture { reader }
    }

    async fn read_1(&mut self) -> IoResult<u8> {
        let mut val = [0];
        self.reader.read_exact(&mut val[..]).await?;
        Ok(val[0])
    }

    async fn read_2(&mut self) -> IoResult<[u8; 2]> {
        unsafe {
            let mut val: [u8; 2] = std::mem::uninitialized();
            self.reader.initializer().initialize(&mut val);
            self.reader.read_exact(&mut val[..]).await?;
            Ok(val)
        }
    }

    async fn read_4(&mut self) -> IoResult<[u8; 4]> {
        unsafe {
            let mut val: [u8; 4] = std::mem::uninitialized();
            self.reader.initializer().initialize(&mut val);
            self.reader.read_exact(&mut val[..]).await?;
            Ok(val)
        }
    }

    async fn read_8(&mut self) -> IoResult<[u8; 8]> {
        unsafe {
            let mut val: [u8; 8] = std::mem::uninitialized();
            self.reader.initializer().initialize(&mut val);
            self.reader.read_exact(&mut val[..]).await?;
            Ok(val)
        }
    }

    async fn read_u8(&mut self) -> IoResult<u8> {
        self.read_1().await
    }

    async fn read_u16(&mut self) -> IoResult<u16> {
        Ok(BigEndian::read_u16(&self.read_2().await?))
    }

    async fn read_u32(&mut self) -> IoResult<u32> {
        Ok(BigEndian::read_u32(&self.read_4().await?))
    }

    async fn read_u64(&mut self) -> IoResult<u64> {
        Ok(BigEndian::read_u64(&self.read_8().await?))
    }

    async fn read_i8(&mut self) -> IoResult<i8> {
        self.read_1().await.map(|x| x as i8)
    }

    async fn read_i16(&mut self) -> IoResult<i16> {
        Ok(BigEndian::read_i16(&self.read_2().await?))
    }

    async fn read_i32(&mut self) -> IoResult<i32> {
        Ok(BigEndian::read_i32(&self.read_4().await?))
    }

    async fn read_i64(&mut self) -> IoResult<i64> {
        Ok(BigEndian::read_i64(&self.read_8().await?))
    }

    pub async fn decode(mut self) -> IoResult<ValueFuture<R>> {
        let marker = Marker::from_u8(self.read_u8().await?);
        Ok(match marker {
            Marker::FixPos(val) => ValueFuture::Integer(Integer::from(val), self.reader),
            Marker::FixNeg(val) => ValueFuture::Integer(Integer::from(val), self.reader),
            Marker::Null => ValueFuture::Nil,
            Marker::True => ValueFuture::Boolean(true, self.reader),
            Marker::False => ValueFuture::Boolean(false, self.reader),
            Marker::U8 => ValueFuture::Integer(Integer::from(self.read_u8().await?), self.reader),
            Marker::U16 => ValueFuture::Integer(Integer::from(self.read_u16().await?), self.reader),
            Marker::U32 => ValueFuture::Integer(Integer::from(self.read_u32().await?), self.reader),
            Marker::U64 => ValueFuture::Integer(Integer::from(self.read_u64().await?), self.reader),
            Marker::I8 => ValueFuture::Integer(Integer::from(self.read_i8().await?), self.reader),
            Marker::I16 => ValueFuture::Integer(Integer::from(self.read_i16().await?), self.reader),
            Marker::I32 => ValueFuture::Integer(Integer::from(self.read_i32().await?), self.reader),
            Marker::I64 => ValueFuture::Integer(Integer::from(self.read_i64().await?), self.reader),
            Marker::FixArray(len) => ValueFuture::Array(ArrayFuture {
                reader: self.reader,
                len: len.into(),
            }),
            _ => panic!("Unhandled type {:?}", marker),
        })
    }
}

#[derive(Debug)]
pub struct ArrayFuture<R> {
    reader: R,
    len: usize,
}

impl<R: AsyncRead + Unpin> AsyncRead for ArrayFuture<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        R::poll_read(Pin::new(&mut self.as_mut().reader), cx, buf)
    }
}

impl<R: AsyncRead + Unpin> ArrayFuture<R> {
    pub fn next(mut self) -> MsgPackOption<MsgPackFuture<Self>, R> {
        if self.len > 0 {
            self.len -= 1;
            MsgPackOption::Some(MsgPackFuture::new(self))
        } else {
            MsgPackOption::End(self.reader)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Cursor;
    use rmpv::Value;

    fn value_to_vec(val: &Value) -> Cursor<Vec<u8>> {
        let mut buf = Cursor::new(Vec::new());
        rmpv::encode::write_value(&mut buf, &val).unwrap();
        buf.set_position(0);
        buf
    }

    #[test]
    fn array() {
        async fn array_test(buf: Cursor<Vec<u8>>) -> IoResult<Vec<u64>> {
            let msg = MsgPackFuture::new(buf);

            let mut vec = Vec::new();
            let mut array = msg.decode().await?.as_array().unwrap();

            while let MsgPackOption::Some(elem) = array.next() {
                let (elem, a) = elem.decode().await?.as_u64().unwrap();
                vec.push(elem);
                array = a;
            }

            Ok(vec)
        }
        let val = Value::from(vec![Value::from(1), 2.into(), 3.into(), 4.into()]);
        let val = value_to_vec(&val);
        let val = futures::executor::LocalPool::new()
            .run_until(array_test(val))
            .unwrap();
        assert_eq!(val, vec![1, 2, 3, 4]);
    }

    #[test]
    fn bool() {
        async fn bool_test(buf: Cursor<Vec<u8>>) -> IoResult<bool> {
            let msg = MsgPackFuture::new(buf);
            let val = msg.decode().await?.as_bool().unwrap();
            Ok(val)
        }

        let val = value_to_vec(&true.into());
        let val = futures::executor::LocalPool::new()
            .run_until(bool_test(val))
            .unwrap();
        assert_eq!(val, true);
    }
}
