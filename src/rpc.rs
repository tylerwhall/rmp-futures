pub mod decode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MsgId(u32);

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

#[derive(Debug, Eq, PartialEq, Primitive)]
pub(crate) enum MsgType {
    Request = 0,
    Response = 1,
    Notification = 2,
}
