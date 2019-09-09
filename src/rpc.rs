pub mod decode;

#[derive(Debug, Eq, PartialEq, Primitive)]
pub(crate) enum MsgType {
    Request = 0,
    Response = 1,
    Notification = 2,
}
