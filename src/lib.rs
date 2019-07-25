#![feature(async_await)]

use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};

use rmp::Marker;
use rmpv::Integer;

use byteorder::{BigEndian, ByteOrder};
use futures::io::ErrorKind;
use futures::io::Result as IoResult;
use futures::prelude::*;

#[derive(Debug)]
pub enum ValueFuture<R> {
    Nil,
    Boolean(bool, R),
    Integer(Integer, R),
    Array(ArrayFuture<R>),
    Map(MapFuture<R>),
    Bin(BinFuture<R>),
    String(StringFuture<R>),
    Ext(ExtFuture<R>),
}

impl<R> ValueFuture<R> {
    pub fn into_bool(self) -> Option<(bool, R)> {
        if let ValueFuture::Boolean(val, r) = self {
            Some((val, r))
        } else {
            None
        }
    }

    pub fn into_bin(self) -> Option<BinFuture<R>> {
        if let ValueFuture::Bin(bin) = self {
            Some(bin)
        } else {
            None
        }
    }

    pub fn into_string(self) -> Option<StringFuture<R>> {
        if let ValueFuture::String(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn into_ext(self) -> Option<ExtFuture<R>> {
        if let ValueFuture::Ext(ext) = self {
            Some(ext)
        } else {
            None
        }
    }

    pub fn into_array(self) -> Option<ArrayFuture<R>> {
        if let ValueFuture::Array(array) = self {
            Some(array)
        } else {
            None
        }
    }

    pub fn into_map(self) -> Option<MapFuture<R>> {
        if let ValueFuture::Map(map) = self {
            Some(map)
        } else {
            None
        }
    }

    pub fn into_u64(self) -> Option<(u64, R)> {
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
            Marker::FixStr(len) => ValueFuture::String(StringFuture(BinFuture {
                reader: self.reader,
                len: len.into(),
            })),
            Marker::Str8 => {
                let len = self.read_u8().await?;
                ValueFuture::String(StringFuture(BinFuture {
                    reader: self.reader,
                    len: len.into(),
                }))
            }
            Marker::Str16 => {
                let len = self.read_u16().await?;
                ValueFuture::String(StringFuture(BinFuture {
                    reader: self.reader,
                    len: len.into(),
                }))
            }
            Marker::Str32 => {
                let len = self.read_u32().await?;
                ValueFuture::String(StringFuture(BinFuture {
                    reader: self.reader,
                    len: len as usize,
                }))
            }
            Marker::Bin8 => {
                let len = self.read_u8().await?;
                ValueFuture::Bin(BinFuture {
                    reader: self.reader,
                    len: len.into(),
                })
            }
            Marker::Bin16 => {
                let len = self.read_u16().await?.into();
                ValueFuture::Bin(BinFuture {
                    reader: self.reader,
                    len,
                })
            }
            Marker::Bin32 => {
                let len = self.read_u32().await? as usize;
                ValueFuture::Bin(BinFuture {
                    reader: self.reader,
                    len,
                })
            }
            Marker::FixArray(len) => ValueFuture::Array(ArrayFuture {
                reader: self.reader,
                len: len.into(),
            }),
            Marker::Array16 => {
                let len = self.read_u16().await?;
                ValueFuture::Array(ArrayFuture {
                    reader: self.reader,
                    len: len.into(),
                })
            }
            Marker::Array32 => {
                let len = self.read_u32().await?;
                ValueFuture::Array(ArrayFuture {
                    reader: self.reader,
                    len: len as usize,
                })
            }
            Marker::FixMap(len) => ValueFuture::Map(MapFuture {
                reader: self.reader,
                len: len.into(),
            }),
            Marker::Map16 => {
                let len = self.read_u16().await?;
                ValueFuture::Map(MapFuture {
                    reader: self.reader,
                    len: len.into(),
                })
            }
            Marker::Map32 => {
                let len = self.read_u32().await?;
                ValueFuture::Map(MapFuture {
                    reader: self.reader,
                    len: len as usize,
                })
            }
            Marker::FixExt1 => {
                let ty = self.read_i8().await?;
                ValueFuture::Ext(ExtFuture {
                    bin: BinFuture {
                        reader: self.reader,
                        len: 1,
                    },
                    ty,
                })
            }
            Marker::FixExt2 => {
                let ty = self.read_i8().await?;
                ValueFuture::Ext(ExtFuture {
                    bin: BinFuture {
                        reader: self.reader,
                        len: 2,
                    },
                    ty,
                })
            }
            Marker::FixExt4 => {
                let ty = self.read_i8().await?;
                ValueFuture::Ext(ExtFuture {
                    bin: BinFuture {
                        reader: self.reader,
                        len: 4,
                    },
                    ty,
                })
            }
            Marker::FixExt8 => {
                let ty = self.read_i8().await?;
                ValueFuture::Ext(ExtFuture {
                    bin: BinFuture {
                        reader: self.reader,
                        len: 8,
                    },
                    ty,
                })
            }
            Marker::FixExt16 => {
                let ty = self.read_i8().await?;
                ValueFuture::Ext(ExtFuture {
                    bin: BinFuture {
                        reader: self.reader,
                        len: 16,
                    },
                    ty,
                })
            }
            Marker::Ext8 => {
                let len = self.read_u8().await?;
                let ty = self.read_i8().await?;
                ValueFuture::Ext(ExtFuture {
                    bin: BinFuture {
                        reader: self.reader,
                        len: len.into(),
                    },
                    ty,
                })
            }
            Marker::Ext16 => {
                let len = self.read_u16().await?;
                let ty = self.read_i8().await?;
                ValueFuture::Ext(ExtFuture {
                    bin: BinFuture {
                        reader: self.reader,
                        len: len.into(),
                    },
                    ty,
                })
            }
            Marker::Ext32 => {
                let len = self.read_u32().await?;
                let ty = self.read_i8().await?;
                ValueFuture::Ext(ExtFuture {
                    bin: BinFuture {
                        reader: self.reader,
                        len: len as usize,
                    },
                    ty,
                })
            }
            Marker::F32 | Marker::F64 => unimplemented!(),
            Marker::Reserved => return Err(ErrorKind::InvalidData.into()),
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

#[derive(Debug)]
pub struct MapFuture<R> {
    reader: R,
    len: usize,
}

impl<R: AsyncRead + Unpin> AsyncRead for MapFuture<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        R::poll_read(Pin::new(&mut self.as_mut().reader), cx, buf)
    }
}

impl<R: AsyncRead + Unpin> MapFuture<R> {
    pub fn next_key(mut self) -> MsgPackOption<MsgPackFuture<MapValueFuture<R>>, R> {
        if self.len > 0 {
            self.len -= 1;
            MsgPackOption::Some(MsgPackFuture::new(MapValueFuture { reader: self }))
        } else {
            MsgPackOption::End(self.reader)
        }
    }
}

#[derive(Debug)]
pub struct MapValueFuture<R> {
    reader: MapFuture<R>,
}

impl<R: AsyncRead + Unpin> AsyncRead for MapValueFuture<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        MapFuture::poll_read(Pin::new(&mut self.as_mut().reader), cx, buf)
    }
}

impl<R: AsyncRead + Unpin> MapValueFuture<R> {
    pub fn next_value(self) -> MsgPackFuture<MapFuture<R>> {
        MsgPackFuture::new(self.reader)
    }
}

#[derive(Debug)]
pub struct BinFuture<R> {
    reader: R,
    len: usize,
}

impl<R: AsyncRead + Unpin> BinFuture<R> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Panics if buf length is less than the value returned by `len()`
    pub async fn read_all(mut self, buf: &mut [u8]) -> IoResult<R> {
        self.reader
            .read_exact(&mut buf[..self.len])
            .await
            .map(|_| self.reader)
    }

    pub fn into_inner(self) -> R {
        assert_eq!(self.len, 0);
        self.reader
    }

    pub async fn into_vec(mut self) -> IoResult<(Vec<u8>, R)> {
        let mut vec = vec![0u8; self.len];
        self.reader
            .read_exact(&mut vec[..])
            .await
            .map(|_| (vec, self.reader))
    }
}

#[derive(Debug)]
pub struct StringFuture<R>(BinFuture<R>);

impl<R: AsyncRead + Unpin> StringFuture<R> {
    pub async fn into_string(self) -> IoResult<(String, R)> {
        self.0.into_vec().await.and_then(|(v, r)| {
            String::from_utf8(v)
                .map_err(|_| ErrorKind::InvalidData.into())
                .map(|s| (s, r))
        })
    }
}

impl<R> Deref for StringFuture<R> {
    type Target = BinFuture<R>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<R> DerefMut for StringFuture<R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct ExtFuture<R> {
    bin: BinFuture<R>,
    ty: i8,
}

impl<R: AsyncRead + Unpin> ExtFuture<R> {
    pub fn ext_type(&self) -> i8 {
        self.ty
    }

    pub async fn into_vec(self) -> IoResult<(i8, Vec<u8>, R)> {
        let ty = self.ty;
        self.bin.into_vec().await.map(|(v, r)| (ty, v, r))
    }
}

impl<R> Deref for ExtFuture<R> {
    type Target = BinFuture<R>;

    fn deref(&self) -> &Self::Target {
        &self.bin
    }
}

impl<R> DerefMut for ExtFuture<R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bin
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rmpv::Value;
    use std::io::Cursor;

    fn value_to_vec(val: &Value) -> Cursor<Vec<u8>> {
        let mut buf = Cursor::new(Vec::new());
        rmpv::encode::write_value(&mut buf, &val).unwrap();
        buf.set_position(0);
        buf
    }

    #[test]
    fn bool() {
        async fn bool_test(buf: Cursor<Vec<u8>>) -> IoResult<bool> {
            let msg = MsgPackFuture::new(buf);
            let (val, _r) = msg.decode().await?.into_bool().unwrap();
            Ok(val)
        }

        let val = value_to_vec(&true.into());
        let val = futures::executor::LocalPool::new()
            .run_until(bool_test(val))
            .unwrap();
        assert_eq!(val, true);
    }

    #[test]
    fn bin() {
        async fn bin_test(buf: Cursor<Vec<u8>>) -> IoResult<Vec<u8>> {
            let msg = MsgPackFuture::new(buf);
            let (val, _r) = msg.decode().await?.into_bin().unwrap().into_vec().await?;
            Ok(val)
        }

        let val = value_to_vec(&vec![1u8, 2, 3, 4].into());
        let val = futures::executor::LocalPool::new()
            .run_until(bin_test(val))
            .unwrap();
        assert_eq!(val, vec![1u8, 2, 3, 4]);
    }

    #[test]
    fn string() {
        async fn string_test(buf: Cursor<Vec<u8>>) -> IoResult<String> {
            let msg = MsgPackFuture::new(buf);
            let (val, _r) = msg
                .decode()
                .await?
                .into_string()
                .unwrap()
                .into_string()
                .await?;
            Ok(val)
        }

        let val = value_to_vec(&"Hello world".into());
        let val = futures::executor::LocalPool::new()
            .run_until(string_test(val))
            .unwrap();
        assert_eq!(val, "Hello world");
    }

    #[test]
    fn array_int() {
        async fn array_test(buf: Cursor<Vec<u8>>) -> IoResult<Vec<u64>> {
            let msg = MsgPackFuture::new(buf);

            let mut vec = Vec::new();
            let mut array = msg.decode().await?.into_array().unwrap();

            while let MsgPackOption::Some(elem) = array.next() {
                let (elem, a) = elem.decode().await?.into_u64().unwrap();
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
    fn map_int() {
        async fn map_test(buf: Cursor<Vec<u8>>) -> IoResult<Vec<(u64, u64)>> {
            let msg = MsgPackFuture::new(buf);

            let mut vec = Vec::new();
            let mut map = msg.decode().await?.into_map().unwrap();

            while let MsgPackOption::Some(elem) = map.next_key() {
                let (key, r) = elem.decode().await?.into_u64().unwrap();
                let (val, m) = r.next_value().decode().await?.into_u64().unwrap();
                vec.push((key, val));
                map = m;
            }

            Ok(vec)
        }
        let val = Value::from(vec![
            (Value::from(1), Value::from(2)),
            (Value::from(3), Value::from(4)),
        ]);
        let val = value_to_vec(&val);
        let val = futures::executor::LocalPool::new()
            .run_until(map_test(val))
            .unwrap();
        assert_eq!(val, vec![(1, 2), (3, 4)]);
    }

    #[test]
    fn ext() {
        async fn ext_test(buf: Cursor<Vec<u8>>) -> IoResult<(i8, Vec<u8>)> {
            let msg = MsgPackFuture::new(buf);
            let (ty, val, _r) = msg.decode().await?.into_ext().unwrap().into_vec().await?;
            Ok((ty, val))
        }

        let val = value_to_vec(&Value::Ext(42, vec![1u8, 2, 3, 4]));
        let val = futures::executor::LocalPool::new()
            .run_until(ext_test(val))
            .unwrap();
        assert_eq!(val, (42, vec![1, 2, 3, 4]));
    }
}
