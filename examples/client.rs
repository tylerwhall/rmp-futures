#![feature(async_await)]

use futures::executor::block_on;
use romio::TcpStream;
use std::io;

use rmp_futures::rpc::decode::RpcMessage;
use rmp_futures::rpc::decode::RpcStream;
use rmp_futures::rpc::encode::RpcSink;

fn main() -> io::Result<()> {
    block_on(async {
        let addr = "127.0.0.1:12345".parse().unwrap();
        let socket = TcpStream::connect(&addr).await?;

        let sink = RpcSink::new(socket);
        let args = sink.write_request(1.into(), "hello", 1).await?;
        let sink = args.last().unwrap().write_str("Bob").await?;

        let stream = RpcStream::new(sink.into_inner());
        match stream.next().await? {
            RpcMessage::Response(resp) => {
                let id = resp.id();
                let result = resp.result().await?;
                match result {
                    Ok(vf) => {
                        let (s, _stream) = vf.into_string().unwrap().into_string().await?;
                        println!("got good response id={:?} s={:?}", id, s);
                    }
                    Err(vf) => {
                        let (s, _stream) = vf.into_string().unwrap().into_string().await?;
                        println!("got bad response id={:?} s={:?}", id, s);
                    }
                }
            }
            _ => panic!("got unexpected msg"),
        };
        Ok(())
    })
}
