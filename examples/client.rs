#![feature(async_await)]

use futures::executor::LocalPool;
use futures::io::AsyncReadExt;
use futures::task::LocalSpawnExt;
use romio::TcpStream;
use std::io;
use std::sync::Arc;

use rmp_futures::rpc::decode::RpcStream;
use rmp_futures::rpc::encode::RpcSink;
use rmp_futures::rpc::RequestDispatch;

fn main() -> io::Result<()> {
    let mut pool = LocalPool::new();
    let mut spawner = pool.spawner();
    pool.run_until(async {
        let addr = "127.0.0.1:12345".parse().unwrap();
        let socket = TcpStream::connect(&addr).await?;

        let (reader, writer) = socket.split();
        let sink = RpcSink::new(writer);
        let stream = RpcStream::new(reader);
        let dispatch = Arc::new(RequestDispatch::default());
        let dispatch2 = dispatch.clone();

        spawner
            .spawn_local(async move {
                dispatch2.dispatch(stream).await.unwrap();
            })
            .unwrap();

        let (id, reply1) = dispatch.new_request();
        let args = sink.write_request(id, "hello", 1).await?;
        let sink = args.last().write_str("Bob").await?;

        let (id, reply2) = dispatch.new_request();
        let args = sink.write_request(id, "hello", 1).await?;
        let _sink = args.last().write_str("Bob").await?;

        for reply in [reply1, reply2].iter_mut() {
            match reply.await.unwrap() {
                Ok(vf) => {
                    let (s, _stream) = vf.into_string().unwrap().into_string().await?;
                    println!("got good response s={:?}", s);
                }
                Err(vf) => {
                    let (s, _stream) = vf.into_string().unwrap().into_string().await?;
                    println!("got bad response s={:?}", s);
                }
            }
        }
        Ok(())
    })
}
