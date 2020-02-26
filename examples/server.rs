use futures::executor::{block_on, ThreadPool};
use futures::io::AsyncRead;
use futures::io::AsyncReadExt;
use futures::io::AsyncWrite;
use futures::lock::Mutex;
use futures::task::SpawnExt;
use futures::StreamExt;
use romio::{TcpListener, TcpStream};
use std::io;
use std::sync::Arc;

use rmp_futures::rpc::decode::RpcMessage;
use rmp_futures::rpc::decode::RpcParamsFuture;
use rmp_futures::rpc::decode::RpcStream;
use rmp_futures::rpc::encode::RpcSink;
use rmp_futures::rpc::MsgId;

async fn hello_handler<W, R>(
    id: MsgId,
    w: Arc<Mutex<Option<W>>>,
    params: RpcParamsFuture<R>,
    mut t: &ThreadPool,
) -> R
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
    t.spawn(async move {
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
    })
    .unwrap();
    reader
}

async fn handler(stream: TcpStream, t: ThreadPool) -> io::Result<()> {
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
                    "hello" => hello_handler(id, w, params, &t).await,
                    _ => panic!("unknown method"),
                }
            }
            RpcMessage::Response(_resp) => panic!("got response"),
            RpcMessage::Notify(_nfy) => panic!("got notify"),
        };
    }
}

fn main() -> io::Result<()> {
    block_on(async {
        let mut threadpool = ThreadPool::new()?;
        let mut listener = TcpListener::bind(&"127.0.0.1:12345".parse().unwrap())?;
        let mut incoming = listener.incoming();

        println!("listening on port 12345");

        while let Some(stream) = incoming.next().await {
            let stream = stream?;
            let t = threadpool.clone();
            threadpool
                .spawn(async move {
                    let addr = stream.peer_addr().unwrap();
                    println!("accepting stream from: {}", addr);
                    match handler(stream, t).await {
                        Ok(_) => println!("it was ok!"),
                        Err(e) => eprintln!("got error: {:?}", e),
                    }
                    println!("closing stream from: {}", addr);
                })
                .unwrap();
        }
        Ok(())
    })
}
