use async_std::net::{TcpListener, TcpStream};
use futures::io::AsyncRead;
use futures::io::AsyncReadExt;
use futures::io::AsyncWrite;
use futures::lock::Mutex;
use futures::StreamExt;
use std::io;
use std::sync::Arc;

use rmp_futures::rpc::decode::RpcMessage;
use rmp_futures::rpc::decode::RpcParamsFuture;
use rmp_futures::rpc::decode::RpcStream;
use rmp_futures::rpc::encode::RpcSink;
use rmp_futures::rpc::MsgId;

async fn hello_handler<W, R>(id: MsgId, w: Arc<Mutex<Option<W>>>, params: RpcParamsFuture<R>) -> R
where
    W: AsyncWrite + Unpin + Send + 'static,
    R: AsyncRead + Unpin,
{
    let params = params.params().await.unwrap();
    let (param1, reader) = params
        .last()
        .unwrap()
        .decode()
        .await
        .unwrap()
        .into_string()
        .unwrap()
        .into_string()
        .await
        .unwrap();
    async_std::task::spawn(async move {
        let mut w = w.lock().await;
        let writer = w.take().unwrap();
        let sink = RpcSink::new(writer);
        let resp = "Hello there ".to_owned() + &param1;
        w.replace(
            sink.write_ok_response(id, |rsp| rsp.write_str(&resp))
                .await
                .unwrap()
                .into_inner(),
        );
        println!("got hello with id={:?} param={}", id, param1);
    });
    reader
}

async fn handler(stream: TcpStream) -> io::Result<()> {
    let (reader, writer) = stream.split();
    let w = Arc::new(Mutex::new(Some(writer)));
    let mut reader = RpcStream::new(reader);
    loop {
        reader = match reader.next().await? {
            RpcMessage::Request(req) => {
                let id = req.id();
                let method = req.method().await?;
                let (method, params) = method.into_string().await?;
                let w = w.clone();
                match method.as_ref() {
                    "hello" => hello_handler(id, w, params).await,
                    _ => panic!("unknown method"),
                }
            }
            RpcMessage::Response(_resp) => panic!("got response"),
            RpcMessage::Notify(_nfy) => panic!("got notify"),
        };
    }
}

#[async_std::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:12345").await?;
    let mut incoming = listener.incoming();

    println!("listening on port 12345");

    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        async_std::task::spawn(async move {
            let addr = stream.peer_addr().unwrap();
            println!("accepting stream from: {}", addr);
            match handler(stream).await {
                Ok(_) => println!("it was ok!"),
                Err(e) => eprintln!("got error: {:?}", e),
            }
            println!("closing stream from: {}", addr);
        });
    }
    Ok(())
}
