use std::convert::TryFrom;

use rmp::Marker;

use byteorder::{BigEndian, ByteOrder};
use futures::io::Result as IoResult;
use futures::prelude::*;

/// The smallest representation of a uint based on its value
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EfficientInt {
    Fix(u8),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
}

impl From<u8> for EfficientInt {
    fn from(val: u8) -> Self {
        if val & 0x7f == val {
            EfficientInt::Fix(val)
        } else {
            EfficientInt::U8(val)
        }
    }
}

impl From<u16> for EfficientInt {
    fn from(val: u16) -> Self {
        if let Ok(val) = u8::try_from(val) {
            val.into()
        } else {
            EfficientInt::U16(val)
        }
    }
}

impl From<u32> for EfficientInt {
    fn from(val: u32) -> Self {
        if let Ok(val) = u16::try_from(val) {
            val.into()
        } else {
            EfficientInt::U32(val)
        }
    }
}

impl From<u64> for EfficientInt {
    fn from(val: u64) -> Self {
        if let Ok(val) = u32::try_from(val) {
            val.into()
        } else {
            EfficientInt::U64(val)
        }
    }
}

#[test]
fn efficient_u8() {
    assert_eq!(EfficientInt::from(1u8), EfficientInt::Fix(1));
    assert_eq!(EfficientInt::from(127u8), EfficientInt::Fix(127));
    assert_eq!(EfficientInt::from(128u8), EfficientInt::U8(128));
    assert_eq!(EfficientInt::from(255u8), EfficientInt::U8(255));
}

#[test]
fn efficient_u16() {
    assert_eq!(EfficientInt::from(1u16), EfficientInt::Fix(1));
    assert_eq!(EfficientInt::from(127u16), EfficientInt::Fix(127));
    assert_eq!(EfficientInt::from(128u16), EfficientInt::U8(128));
    assert_eq!(EfficientInt::from(255u16), EfficientInt::U8(255));
    assert_eq!(EfficientInt::from(256u16), EfficientInt::U16(256));
    assert_eq!(EfficientInt::from(65535u16), EfficientInt::U16(65535));
}

#[test]
fn efficient_u32() {
    assert_eq!(EfficientInt::from(1u32), EfficientInt::Fix(1));
    assert_eq!(EfficientInt::from(127u32), EfficientInt::Fix(127));
    assert_eq!(EfficientInt::from(128u32), EfficientInt::U8(128));
    assert_eq!(EfficientInt::from(255u32), EfficientInt::U8(255));
    assert_eq!(EfficientInt::from(256u32), EfficientInt::U16(256));
    assert_eq!(EfficientInt::from(65535u32), EfficientInt::U16(65535));
    assert_eq!(EfficientInt::from(65536u32), EfficientInt::U32(65536));
    assert_eq!(
        EfficientInt::from(4_294_967_295u32),
        EfficientInt::U32(4_294_967_295)
    );
}

#[test]
fn efficient_u64() {
    assert_eq!(EfficientInt::from(1u64), EfficientInt::Fix(1));
    assert_eq!(EfficientInt::from(127u64), EfficientInt::Fix(127));
    assert_eq!(EfficientInt::from(128u64), EfficientInt::U8(128));
    assert_eq!(EfficientInt::from(255u64), EfficientInt::U8(255));
    assert_eq!(EfficientInt::from(256u64), EfficientInt::U16(256));
    assert_eq!(EfficientInt::from(65535u64), EfficientInt::U16(65535));
    assert_eq!(EfficientInt::from(65536u64), EfficientInt::U32(65536));
    assert_eq!(
        EfficientInt::from(4_294_967_295u64),
        EfficientInt::U32(4_294_967_295)
    );
    assert_eq!(
        EfficientInt::from(4_294_967_296u64),
        EfficientInt::U64(4_294_967_296)
    );
    assert_eq!(
        EfficientInt::from(std::u64::MAX),
        EfficientInt::U64(std::u64::MAX)
    );
}

pub struct MsgPackSink<W> {
    writer: W,
}

impl<W: AsyncWrite + Unpin> MsgPackSink<W> {
    pub fn new(writer: W) -> Self {
        MsgPackSink { writer }
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    async fn write_1(&mut self, val: [u8; 1]) -> IoResult<()> {
        self.writer.write_all(&val).await
    }

    async fn write_2(&mut self, val: [u8; 2]) -> IoResult<()> {
        self.writer.write_all(&val).await
    }

    async fn write_4(&mut self, val: [u8; 4]) -> IoResult<()> {
        self.writer.write_all(&val).await
    }

    async fn write_8(&mut self, val: [u8; 8]) -> IoResult<()> {
        self.writer.write_all(&val).await
    }

    async fn write_u8(&mut self, val: u8) -> IoResult<()> {
        let buf = [val];
        self.write_1(buf).await
    }

    async fn write_u16(&mut self, val: u16) -> IoResult<()> {
        let mut buf = [0u8; 2];
        BigEndian::write_u16(&mut buf, val);
        self.write_2(buf).await
    }

    async fn write_u32(&mut self, val: u32) -> IoResult<()> {
        let mut buf = [0u8; 4];
        BigEndian::write_u32(&mut buf, val);
        self.write_4(buf).await
    }

    async fn write_u64(&mut self, val: u64) -> IoResult<()> {
        let mut buf = [0u8; 8];
        BigEndian::write_u64(&mut buf, val);
        self.write_8(buf).await
    }

    async fn write_i8(&mut self, val: i8) -> IoResult<()> {
        let buf = [val as u8];
        self.write_1(buf).await
    }

    async fn write_i16(&mut self, val: i16) -> IoResult<()> {
        let mut buf = [0u8; 2];
        BigEndian::write_i16(&mut buf, val);
        self.write_2(buf).await
    }

    async fn write_i32(&mut self, val: i32) -> IoResult<()> {
        let mut buf = [0u8; 4];
        BigEndian::write_i32(&mut buf, val);
        self.write_4(buf).await
    }

    async fn write_i64(&mut self, val: i64) -> IoResult<()> {
        let mut buf = [0u8; 8];
        BigEndian::write_i64(&mut buf, val);
        self.write_8(buf).await
    }

    async fn write_marker(&mut self, marker: Marker) -> IoResult<()> {
        self.write_u8(marker.to_u8()).await
    }

    pub async fn write_nil(&mut self) -> IoResult<()> {
        self.write_marker(Marker::Null).await
    }

    pub async fn write_bool(&mut self, val: bool) -> IoResult<()> {
        if val {
            self.write_marker(Marker::True).await
        } else {
            self.write_marker(Marker::False).await
        }
    }

    async fn write_efficient_uint(&mut self, val: EfficientInt) -> IoResult<()> {
        match val {
            EfficientInt::Fix(val) => self.write_marker(Marker::FixPos(val)).await?,
            EfficientInt::U8(val) => {
                self.write_marker(Marker::U8).await?;
                self.write_u8(val).await?;
            }
            EfficientInt::U16(val) => {
                self.write_marker(Marker::U16).await?;
                self.write_u16(val).await?;
            }
            EfficientInt::U32(val) => {
                self.write_marker(Marker::U32).await?;
                self.write_u32(val).await?;
            }
            EfficientInt::U64(val) => {
                self.write_marker(Marker::U64).await?;
                self.write_u64(val).await?;
            }
        }
        Ok(())
    }

    /// Write any unsigned int (u8-u64) in the most efficient representation
    pub async fn write_uint(&mut self, val: impl Into<EfficientInt>) -> IoResult<()> {
        self.write_efficient_uint(val.into()).await
    }
}

#[cfg(test)]
fn run_future<R>(f: impl Future<Output = R>) -> R {
    futures::executor::LocalPool::new().run_until(f)
}

#[test]
fn efficient_uint() {
    fn test_against_rmpv<V: Into<u64> + Into<EfficientInt> + Copy>(val: V) {
        use std::io::Cursor;

        let mut c1 = Cursor::new(vec![0; 9]);
        rmp::encode::write_uint(&mut c1, val.into()).unwrap();
        let m1 = c1.into_inner();

        let mut msg = MsgPackSink::new(Cursor::new(vec![0; 9]));
        run_future(msg.write_uint(val)).unwrap();
        let m2 = msg.into_inner().into_inner();

        assert_eq!(m1, m2);
    }

    test_against_rmpv(1u8);
    test_against_rmpv(127u8);
    test_against_rmpv(128u8);
    test_against_rmpv(255u8);

    test_against_rmpv(1u16);
    test_against_rmpv(127u16);
    test_against_rmpv(128u16);
    test_against_rmpv(255u16);
    test_against_rmpv(256u16);
    test_against_rmpv(65535u16);

    test_against_rmpv(1u32);
    test_against_rmpv(127u32);
    test_against_rmpv(128u32);
    test_against_rmpv(255u32);
    test_against_rmpv(256u32);
    test_against_rmpv(65535u32);
    test_against_rmpv(65536u32);
    test_against_rmpv(4_294_967_295u32);

    test_against_rmpv(1u64);
    test_against_rmpv(127u64);
    test_against_rmpv(128u64);
    test_against_rmpv(255u64);
    test_against_rmpv(256u64);
    test_against_rmpv(65535u64);
    test_against_rmpv(65536u64);
    test_against_rmpv(4_294_967_295u64);
    test_against_rmpv(4_294_967_296u64);
    test_against_rmpv(std::u64::MAX);
}
