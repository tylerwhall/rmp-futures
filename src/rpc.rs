pub mod decode;
pub mod encode;
mod shared_reader;
pub mod shared_writer;

pub use shared_reader::{RequestDispatch, RpcIncomingMessage};

use crate::encode::EfficientInt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MsgId(u32);

impl From<MsgId> for EfficientInt {
    fn from(msgid: MsgId) -> EfficientInt {
        msgid.0.into()
    }
}

// Allow getting, but not modifying, the raw id
impl From<MsgId> for u32 {
    fn from(msgid: MsgId) -> u32 {
        msgid.0
    }
}

impl From<u32> for MsgId {
    fn from(id: u32) -> Self {
        MsgId(id)
    }
}

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Primitive)]
pub(crate) enum MsgType {
    Request = 0,
    Response = 1,
    Notification = 2,
}

impl From<MsgType> for EfficientInt {
    fn from(ty: MsgType) -> EfficientInt {
        (ty as u8).into()
    }
}
