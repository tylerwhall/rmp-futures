#![feature(async_await)]
// async syntax confuses clippy (2019/07/30)
#![allow(clippy::needless_lifetimes)]

pub mod decode;
pub mod encode;
