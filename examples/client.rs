#![feature(async_await)]

use futures::executor::block_on;
use romio::TcpStream;
use std::io;

use rmp_futures::encode::MsgPackWriter;
use rmp_futures::rpc::decode::RpcMessage;
use rmp_futures::rpc::decode::RpcStream;

fn main() -> io::Result<()> {
    block_on(async {
        let addr = "127.0.0.1:12345".parse().unwrap();
        let socket = TcpStream::connect(&addr).await?;

        let sink = MsgPackWriter::new(socket);
        let args = sink
            .write_array_len(4)
            .await?
            .next()
            .unwrap()
            .write_int(0)
            .await?
            .next()
            .unwrap()
            .write_int(1)
            .await?
            .next()
            .unwrap()
            .write_str("hello")
            .await?
            .next()
            .unwrap();
        let socket = args
            .write_array_len(1)
            .await?
            .next()
            .unwrap()
            .write_str("Bob")
            .await?
            .next()
            .unwrap_end()
            .next()
            .unwrap_end();

        let stream = RpcStream::new(socket);
        match stream.next().await? {
            RpcMessage::Response(resp) => {
                let id = resp.id();
                let result = resp.result().await?;
                match result {
                    Ok(vf) => {
                        let (s, _stream) = vf.into_string().unwrap().into_string().await?;
                        println!("got good response id={} s={:?}", id, s);
                    }
                    Err(vf) => {
                        let (s, _stream) = vf.into_string().unwrap().into_string().await?;
                        println!("got bad response id={} s={:?}", id, s);
                    }
                }
            }
            _ => panic!("got unexpected msg"),
        };
        Ok(())
    })
}
