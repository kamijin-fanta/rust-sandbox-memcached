#![warn(rust_2018_idioms)]

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // TCPサーバの起動
    // https://github.com/tokio-rs/tokio/blob/master/examples/echo.rs

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);

    let mut server = Server::new();
    server.run(listener).await
}

struct Server {}

impl Server {
    fn new() -> Self {
        Server {}
    }

    async fn run(&mut self, listener: TcpListener) -> Result<(), Box<dyn Error>> {
        let kvs: HashMap<String, Vec<u8>> = HashMap::new();
        let kvs = Arc::new(Mutex::new(kvs));

        loop {
            // TcpStreamをBufStreamへ変換
            // https://users.rust-lang.org/t/how-can-i-read-line-by-line-from-tokio-tcpstream/38665
            // https://v0-1--tokio.netlify.app/docs/io/reading_writing_data/
            let (socket, _) = listener.accept().await?;
            let mut stream = BufStream::new(socket);
            let kvs = kvs.clone();

            tokio::spawn(async move {
                let r = async {
                    loop {
                        let mut line = String::new();
                        stream.read_line(&mut line).await?;
                        if line.len() == 0 {
                            // Client closed
                            // https://rust-lang.github.io/async-book/07_workarounds/02_err_in_async_blocks.html
                            return Ok::<(), Box<dyn Error>>(())
                        }
                        let line = &line[..line.len() - 1];

                        // Vecにマッチングは書けないっぽいのでSliceへ変換
                        // https://stackoverflow.com/questions/9282805/rust-pattern-matching-over-a-vector
                        match line.split(' ').collect::<Vec<_>>()[..] {
                            ["get", key] => {
                                println!("get {}", key);

                                let result = {
                                    kvs.lock().unwrap().get(key).cloned()
                                };

                                match result {
                                    Some(value) => {
                                        stream.write_all(format!("VALUE {} 0 {}\r\n", key, value.len()).as_bytes()).await.unwrap();
                                        stream.write_all(&value).await.unwrap();
                                        stream.write_all("\r\nEND\r\n".as_bytes()).await?
                                    }
                                    None => stream.write_all("END\r\n".as_bytes()).await?,
                                };
                            }
                            ["set", key, flags, exptime, length] => {
                                println!("set key={} flags={} exptime={} length={}", key, flags, exptime, length);

                                let len = length.parse().unwrap_or(0);
                                let mut buf = vec![0; len];
                                stream.read_exact(buf.as_mut_slice()).await?;

                                kvs.lock().unwrap().insert(key.to_string(), buf);

                                stream.read_line(&mut String::new()).await?; // 最後に余った改行コードを読む
                                stream.write_all("STORED\r\n".as_bytes()).await?;
                            }
                            ["append", key, flags, exptime, length] => {
                                println!("append key={} flags={} exptime={} length={}", key, flags, exptime, length);

                                let len = length.parse().unwrap_or(0);
                                let mut buf = vec![0; len];
                                stream.read_exact(buf.as_mut_slice()).await?;

                                let stored = {
                                    let mut k = kvs.lock()?;
                                    match k.get_mut(key) {
                                        Some(current) => {
                                            current.append(&mut buf);
                                            true
                                        }
                                        None => false
                                    }
                                };

                                stream.read_line(&mut String::new()).await?; // 最後に余った改行コードを読む

                                if stored {
                                    stream.write_all("STORED\r\n".as_bytes()).await?;
                                } else {
                                    stream.write_all("NOT_STORED\r\n".as_bytes()).await?;
                                }
                            }
                            ["delete", key] => {
                                println!("delete {}", key);

                                let deleted = {
                                    match kvs.lock().unwrap().remove(key) {
                                        Some(_) => true,
                                        None => false,
                                    }
                                };
                                if deleted {
                                    stream.write_all("DELETED\r\n".as_bytes()).await?;
                                } else {
                                    stream.write_all("NOT_FOUND\r\n".as_bytes()).await?;
                                }
                            }
                            _ => {
                                println!("invalid command \"{}\"", line.trim());
                                stream.write_all("ERROR".as_bytes()).await?;
                            }
                        }
                        stream.flush().await?;
                    }
                };
                match r.await {
                    Err(err) => println!("error {}", err),
                    Ok(_) => println!("client close")
                }
            });
        }
    }
}
