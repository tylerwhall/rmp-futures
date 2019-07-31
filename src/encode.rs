use std::convert::TryFrom;
use std::convert::TryInto;

use rmp::Marker;

use byteorder::{BigEndian, ByteOrder};
use futures::io::Result as IoResult;
use futures::prelude::*;

/// The smallest representation of a uint based on its value
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EfficientInt {
    FixPos(u8),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    FixNeg(i8),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
}

impl From<u8> for EfficientInt {
    fn from(val: u8) -> Self {
        if val & 0x7f == val {
            EfficientInt::FixPos(val)
        } else {
            EfficientInt::U8(val)
        }
    }
}

impl From<i8> for EfficientInt {
    fn from(val: i8) -> Self {
        if let Ok(val) = u8::try_from(val) {
            val.into()
        } else if val as u8 & 0b1110_0000 == 0b1110_0000 {
            EfficientInt::FixNeg(val)
        } else {
            EfficientInt::I8(val)
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

impl From<i16> for EfficientInt {
    fn from(val: i16) -> Self {
        if let Ok(val) = u16::try_from(val) {
            val.into()
        } else if let Ok(val) = i8::try_from(val) {
            val.into()
        } else {
            EfficientInt::I16(val)
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

impl From<i32> for EfficientInt {
    fn from(val: i32) -> Self {
        if let Ok(val) = u32::try_from(val) {
            val.into()
        } else if let Ok(val) = i16::try_from(val) {
            val.into()
        } else {
            EfficientInt::I32(val)
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

impl From<i64> for EfficientInt {
    fn from(val: i64) -> Self {
        if let Ok(val) = u64::try_from(val) {
            val.into()
        } else if let Ok(val) = i32::try_from(val) {
            val.into()
        } else {
            EfficientInt::I64(val)
        }
    }
}

#[test]
fn efficient_u8() {
    assert_eq!(EfficientInt::from(1u8), EfficientInt::FixPos(1));
    assert_eq!(EfficientInt::from(127u8), EfficientInt::FixPos(127));
    assert_eq!(EfficientInt::from(128u8), EfficientInt::U8(128));
    assert_eq!(EfficientInt::from(255u8), EfficientInt::U8(255));
}

#[test]
fn efficient_i8() {
    assert_eq!(EfficientInt::from(1i8), EfficientInt::FixPos(1));
    assert_eq!(EfficientInt::from(-1i8), EfficientInt::FixNeg(-1));
    assert_eq!(EfficientInt::from(-32i8), EfficientInt::FixNeg(-32));
    assert_eq!(EfficientInt::from(-33i8), EfficientInt::I8(-33));
    assert_eq!(EfficientInt::from(127i8), EfficientInt::FixPos(127));
    assert_eq!(EfficientInt::from(-128i8), EfficientInt::I8(-128));
}

#[test]
fn efficient_u16() {
    assert_eq!(EfficientInt::from(1u16), EfficientInt::FixPos(1));
    assert_eq!(EfficientInt::from(127u16), EfficientInt::FixPos(127));
    assert_eq!(EfficientInt::from(128u16), EfficientInt::U8(128));
    assert_eq!(EfficientInt::from(255u16), EfficientInt::U8(255));
    assert_eq!(EfficientInt::from(256u16), EfficientInt::U16(256));
    assert_eq!(EfficientInt::from(65535u16), EfficientInt::U16(65535));
}

#[test]
fn efficient_i16() {
    assert_eq!(EfficientInt::from(1i16), EfficientInt::FixPos(1));
    assert_eq!(EfficientInt::from(-1i16), EfficientInt::FixNeg(-1));
    assert_eq!(EfficientInt::from(-32i16), EfficientInt::FixNeg(-32));
    assert_eq!(EfficientInt::from(-33i16), EfficientInt::I8(-33));
    assert_eq!(EfficientInt::from(127i16), EfficientInt::FixPos(127));
    assert_eq!(EfficientInt::from(128i16), EfficientInt::U8(128));
    assert_eq!(EfficientInt::from(-128i16), EfficientInt::I8(-128));
    assert_eq!(EfficientInt::from(-129i16), EfficientInt::I16(-129));
    assert_eq!(EfficientInt::from(255i16), EfficientInt::U8(255));
    assert_eq!(EfficientInt::from(256i16), EfficientInt::U16(256));
    assert_eq!(EfficientInt::from(-32768i16), EfficientInt::I16(-32768));
}

#[test]
fn efficient_u32() {
    assert_eq!(EfficientInt::from(1u32), EfficientInt::FixPos(1));
    assert_eq!(EfficientInt::from(127u32), EfficientInt::FixPos(127));
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
fn efficient_i32() {
    assert_eq!(EfficientInt::from(1i32), EfficientInt::FixPos(1));
    assert_eq!(EfficientInt::from(-1i32), EfficientInt::FixNeg(-1));
    assert_eq!(EfficientInt::from(-32i32), EfficientInt::FixNeg(-32));
    assert_eq!(EfficientInt::from(-33i32), EfficientInt::I8(-33));
    assert_eq!(EfficientInt::from(127i32), EfficientInt::FixPos(127));
    assert_eq!(EfficientInt::from(128i32), EfficientInt::U8(128));
    assert_eq!(EfficientInt::from(-128i32), EfficientInt::I8(-128));
    assert_eq!(EfficientInt::from(-129i32), EfficientInt::I16(-129));
    assert_eq!(EfficientInt::from(255i32), EfficientInt::U8(255));
    assert_eq!(EfficientInt::from(256i32), EfficientInt::U16(256));
    assert_eq!(EfficientInt::from(-32768i32), EfficientInt::I16(-32768));
    assert_eq!(EfficientInt::from(-32769i32), EfficientInt::I32(-32769));
    assert_eq!(EfficientInt::from(65535i32), EfficientInt::U16(65535));
    assert_eq!(EfficientInt::from(65536i32), EfficientInt::U32(65536));
    assert_eq!(
        EfficientInt::from(-2_147_483_648i32),
        EfficientInt::I32(-2_147_483_648i32)
    );
}

#[test]
fn efficient_u64() {
    assert_eq!(EfficientInt::from(1u64), EfficientInt::FixPos(1));
    assert_eq!(EfficientInt::from(127u64), EfficientInt::FixPos(127));
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

#[test]
fn efficient_i64() {
    assert_eq!(EfficientInt::from(1i64), EfficientInt::FixPos(1));
    assert_eq!(EfficientInt::from(-1i64), EfficientInt::FixNeg(-1));
    assert_eq!(EfficientInt::from(-32i64), EfficientInt::FixNeg(-32));
    assert_eq!(EfficientInt::from(-33i64), EfficientInt::I8(-33));
    assert_eq!(EfficientInt::from(127i64), EfficientInt::FixPos(127));
    assert_eq!(EfficientInt::from(128i64), EfficientInt::U8(128));
    assert_eq!(EfficientInt::from(-128i64), EfficientInt::I8(-128));
    assert_eq!(EfficientInt::from(-129i64), EfficientInt::I16(-129));
    assert_eq!(EfficientInt::from(255i64), EfficientInt::U8(255));
    assert_eq!(EfficientInt::from(256i64), EfficientInt::U16(256));
    assert_eq!(EfficientInt::from(-32768i64), EfficientInt::I16(-32768));
    assert_eq!(EfficientInt::from(-32769i64), EfficientInt::I32(-32769));
    assert_eq!(EfficientInt::from(65535i64), EfficientInt::U16(65535));
    assert_eq!(EfficientInt::from(65536i64), EfficientInt::U32(65536));
    assert_eq!(
        EfficientInt::from(-2_147_483_648i64),
        EfficientInt::I32(-2_147_483_648i32)
    );
    assert_eq!(
        EfficientInt::from(4_294_967_295i64),
        EfficientInt::U32(4_294_967_295)
    );
    assert_eq!(
        EfficientInt::from(4_294_967_296i64),
        EfficientInt::U64(4_294_967_296)
    );
    assert_eq!(
        EfficientInt::from(std::i64::MIN),
        EfficientInt::I64(std::i64::MIN)
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

    async fn write_efficient_int(&mut self, val: EfficientInt) -> IoResult<()> {
        match val {
            EfficientInt::FixPos(val) => self.write_marker(Marker::FixPos(val)).await?,
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
            EfficientInt::FixNeg(val) => self.write_marker(Marker::FixNeg(val)).await?,
            EfficientInt::I8(val) => {
                self.write_marker(Marker::I8).await?;
                self.write_i8(val).await?;
            }
            EfficientInt::I16(val) => {
                self.write_marker(Marker::I16).await?;
                self.write_i16(val).await?;
            }
            EfficientInt::I32(val) => {
                self.write_marker(Marker::I32).await?;
                self.write_i32(val).await?;
            }
            EfficientInt::I64(val) => {
                self.write_marker(Marker::I64).await?;
                self.write_i64(val).await?;
            }
        }
        Ok(())
    }

    /// Write any int (u8-u64,i8-i64) in the most efficient representation
    pub async fn write_int(&mut self, val: impl Into<EfficientInt>) -> IoResult<()> {
        self.write_efficient_int(val.into()).await
    }

    pub async fn write_f32(&mut self, val: f32) -> IoResult<()> {
        self.write_marker(Marker::F32).await?;
        let mut buf = [0u8; 4];
        BigEndian::write_f32(&mut buf, val);
        self.write_4(buf).await
    }

    pub async fn write_f64(&mut self, val: f64) -> IoResult<()> {
        self.write_marker(Marker::F64).await?;
        let mut buf = [0u8; 8];
        BigEndian::write_f64(&mut buf, val);
        self.write_8(buf).await
    }

    pub async fn write_array_len(&mut self, len: u32) -> IoResult<()> {
        const U16MAX: u32 = std::u16::MAX as u32;

        match len {
            0..=15 => self.write_marker(Marker::FixArray(len as u8)).await,
            16..=U16MAX => {
                self.write_marker(Marker::Array16).await?;
                self.write_u16(len as u16).await
            }
            _ => {
                self.write_marker(Marker::Array32).await?;
                self.write_u32(len).await
            }
        }
    }

    pub async fn write_map_len(&mut self, len: u32) -> IoResult<()> {
        const U16MAX: u32 = std::u16::MAX as u32;

        match len {
            0..=15 => self.write_marker(Marker::FixMap(len as u8)).await,
            16..=U16MAX => {
                self.write_marker(Marker::Map16).await?;
                self.write_u16(len as u16).await
            }
            _ => {
                self.write_marker(Marker::Map32).await?;
                self.write_u32(len).await
            }
        }
    }

    /// Encodes and attempts to write the most efficient binary array length
    /// representation
    pub async fn write_bin_len(&mut self, len: u32) -> IoResult<()> {
        if let Ok(len) = u8::try_from(len) {
            self.write_marker(Marker::Bin8).await?;
            self.write_u8(len).await
        } else if let Ok(len) = u16::try_from(len) {
            self.write_marker(Marker::Bin16).await?;
            self.write_u16(len).await
        } else {
            self.write_marker(Marker::Bin32).await?;
            self.write_u32(len).await
        }
    }

    /// Encodes and attempts to write the most efficient binary representation
    pub async fn write_bin(&mut self, data: &[u8]) -> IoResult<()> {
        self.write_bin_len(data.len().try_into().unwrap()).await?;
        self.writer.write_all(data).await
    }

    /// Encodes and attempts to write the most efficient binary array length
    /// representation
    pub async fn write_str_len(&mut self, len: u32) -> IoResult<()> {
        if let Ok(len) = u8::try_from(len) {
            if len < 32 {
                self.write_marker(Marker::FixStr(len)).await
            } else {
                self.write_marker(Marker::Str8).await?;
                self.write_u8(len).await
            }
        } else if let Ok(len) = u16::try_from(len) {
            self.write_marker(Marker::Str16).await?;
            self.write_u16(len).await
        } else {
            self.write_marker(Marker::Str32).await?;
            self.write_u32(len).await
        }
    }

    /// Encodes and attempts to write the most efficient binary representation
    pub async fn write_str(&mut self, string: &str) -> IoResult<()> {
        self.write_str_len(string.len().try_into().unwrap()).await?;
        self.writer.write_all(string.as_bytes()).await
    }

    /// Encodes and attempts to write the most efficient ext metadata
    /// representation
    ///
    /// # Panics
    ///
    /// Panics if `ty` is negative, because it is reserved for future MessagePack
    /// extension including 2-byte type information.
    pub async fn write_ext_meta(&mut self, len: u32, ty: i8) -> IoResult<()> {
        assert!(ty >= 0);

        if let Ok(len) = u8::try_from(len) {
            match len {
                1 => {
                    self.write_marker(Marker::FixExt1).await?;
                }
                2 => {
                    self.write_marker(Marker::FixExt2).await?;
                }
                4 => {
                    self.write_marker(Marker::FixExt4).await?;
                }
                8 => {
                    self.write_marker(Marker::FixExt8).await?;
                }
                16 => {
                    self.write_marker(Marker::FixExt16).await?;
                }
                len => {
                    self.write_marker(Marker::Ext8).await?;
                    self.write_u8(len).await?;
                }
            }
        } else if let Ok(len) = u16::try_from(len) {
            self.write_marker(Marker::Ext16).await?;
            self.write_u16(len).await?;
        } else {
            self.write_marker(Marker::Ext32).await?;
            self.write_u32(len).await?;
        }
        self.write_u8(ty as u8).await
    }

    pub async fn write_ext(&mut self, data: &[u8], ty: i8) -> IoResult<()> {
        self.write_ext_meta(data.len().try_into().unwrap(), ty)
            .await?;
        self.writer.write_all(data).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn run_future<R>(f: impl Future<Output = R>) -> R {
        futures::executor::LocalPool::new().run_until(f)
    }

    /// Create a 2 writable cursors and wrap one in a `MsgPackSink` call a
    /// function to write into each and compare the contents for equality.
    fn test_jig<F>(f: F)
    where
        F: FnOnce(&mut Cursor<Vec<u8>>, &mut MsgPackSink<Cursor<Vec<u8>>>),
    {
        let mut c1 = Cursor::new(vec![0; 256]);
        let mut msg = MsgPackSink::new(Cursor::new(vec![0; 256]));
        f(&mut c1, &mut msg);
        let m1 = c1.into_inner();
        let m2 = msg.into_inner().into_inner();

        assert_eq!(m1, m2);
    }

    #[test]
    fn nil() {
        test_jig(|c1, msg| {
            rmp::encode::write_nil(c1).unwrap();
            run_future(msg.write_nil()).unwrap();
        });
    }

    #[test]
    fn bool() {
        test_jig(|c1, msg| {
            rmp::encode::write_bool(c1, true).unwrap();
            run_future(msg.write_bool(true)).unwrap();
        });
        test_jig(|c1, msg| {
            rmp::encode::write_bool(c1, false).unwrap();
            run_future(msg.write_bool(false)).unwrap();
        });
    }

    #[test]
    fn float() {
        test_jig(|c1, msg| {
            rmp::encode::write_f32(c1, 1.1).unwrap();
            run_future(msg.write_f32(1.1)).unwrap();
        });
        test_jig(|c1, msg| {
            rmp::encode::write_f64(c1, 1.1).unwrap();
            run_future(msg.write_f64(1.1)).unwrap();
        });
    }

    #[test]
    fn array_len() {
        for i in &[0, 1, 15, 16, 65535, 65536, std::u32::MAX] {
            test_jig(|c1, msg| {
                rmp::encode::write_array_len(c1, *i).unwrap();
                run_future(msg.write_array_len(*i)).unwrap();
            });
        }
    }

    #[test]
    fn map_len() {
        for i in &[0, 1, 15, 16, 65535, 65536, std::u32::MAX] {
            test_jig(|c1, msg| {
                rmp::encode::write_map_len(c1, *i).unwrap();
                run_future(msg.write_map_len(*i)).unwrap();
            });
        }
    }

    #[test]
    fn bin() {
        for i in &[0, 1, 255, 256, 65535, 65536, std::u32::MAX] {
            test_jig(|c1, msg| {
                rmp::encode::write_bin_len(c1, *i).unwrap();
                run_future(msg.write_bin_len(*i)).unwrap();
            });
        }
        test_jig(|c1, msg| {
            let buf = [1, 2, 3, 4];
            rmp::encode::write_bin(c1, &buf).unwrap();
            run_future(msg.write_bin(&buf)).unwrap();
        });
    }

    #[test]
    fn ext() {
        for i in &[0, 1, 2, 4, 8, 16, 17, 255, 256, 65535, 65536, std::u32::MAX] {
            test_jig(|c1, msg| {
                rmp::encode::write_ext_meta(c1, *i, 42).unwrap();
                run_future(msg.write_ext_meta(*i, 42)).unwrap();
            });
        }
    }

    #[test]
    fn string() {
        for i in &[0, 1, 31, 32, 255, 256, 65535, 65536, std::u32::MAX] {
            test_jig(|c1, msg| {
                rmp::encode::write_str_len(c1, *i).unwrap();
                run_future(msg.write_str_len(*i)).unwrap();
            });
        }
        test_jig(|c1, msg| {
            rmp::encode::write_str(c1, "hello").unwrap();
            run_future(msg.write_str("hello")).unwrap();
        });
    }

    #[test]
    fn efficient_uint() {
        fn test_against_rmpv<V: Into<u64> + Into<EfficientInt> + Copy>(val: V) {
            test_jig(|c1, msg| {
                rmp::encode::write_uint(c1, val.into()).unwrap();
                run_future(msg.write_int(val)).unwrap();
            })
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

    #[test]
    fn efficient_int() {
        fn test_against_rmpv<V: Into<i64> + Into<EfficientInt> + Copy>(val: V) {
            test_jig(|c1, msg| {
                rmp::encode::write_sint(c1, val.into()).unwrap();
                run_future(msg.write_int(val)).unwrap();
            })
        }

        test_against_rmpv(1i8);
        test_against_rmpv(-1i8);
        test_against_rmpv(-32i8);
        test_against_rmpv(-33i8);
        test_against_rmpv(127i8);
        test_against_rmpv(-128i8);

        test_against_rmpv(1i16);
        test_against_rmpv(-1i16);
        test_against_rmpv(-32i16);
        test_against_rmpv(-33i16);
        test_against_rmpv(127i16);
        test_against_rmpv(128i16);
        test_against_rmpv(-128i16);
        test_against_rmpv(-129i16);
        test_against_rmpv(255i16);
        test_against_rmpv(256i16);
        test_against_rmpv(-32768i16);

        test_against_rmpv(1i32);
        test_against_rmpv(-1i32);
        test_against_rmpv(-32i32);
        test_against_rmpv(-33i32);
        test_against_rmpv(127i32);
        test_against_rmpv(128i32);
        test_against_rmpv(-128i32);
        test_against_rmpv(-129i32);
        test_against_rmpv(255i32);
        test_against_rmpv(256i32);
        test_against_rmpv(-32768i32);
        test_against_rmpv(-32769i32);
        test_against_rmpv(65535i32);
        test_against_rmpv(65536i32);
        test_against_rmpv(-2_147_483_648i32);

        test_against_rmpv(1i64);
        test_against_rmpv(-1i64);
        test_against_rmpv(-32i64);
        test_against_rmpv(-33i64);
        test_against_rmpv(127i64);
        test_against_rmpv(128i64);
        test_against_rmpv(-128i64);
        test_against_rmpv(-129i64);
        test_against_rmpv(255i64);
        test_against_rmpv(256i64);
        test_against_rmpv(-32768i64);
        test_against_rmpv(-32769i64);
        test_against_rmpv(65535i64);
        test_against_rmpv(65536i64);
        test_against_rmpv(-2_147_483_648i64);
        test_against_rmpv(-2_147_483_649i64);
        test_against_rmpv(4_294_967_295i64);
        test_against_rmpv(4_294_967_296i64);
        test_against_rmpv(std::i64::MIN);
    }

}
