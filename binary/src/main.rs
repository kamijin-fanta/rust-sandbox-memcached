#![warn(rust_2018_idioms)]

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io::Error as IoError;
use std::sync::{Arc, Mutex};

use num_enum::{IntoPrimitive, TryFromPrimitive};
use tokio::io::BufStream;
use tokio::net::{TcpListener, TcpStream};
use tokio_byteorder::BigEndian;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // TCPサーバの起動
    // https://github.com/tokio-rs/tokio/blob/master/examples/echo.rs

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:11211".to_string());

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
        let kvs: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let kvs = Arc::new(Mutex::new(kvs));

        loop {
            let (socket, _) = listener.accept().await?;
            let mut stream = BufStream::new(socket);
            let kvs = kvs.clone();

            tokio::spawn(async move {
                let r = async {
                    loop {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};

                        let req_header = match read_request_header(&mut stream).await {
                            Err(e) => return Err::<(), Box<dyn Error>>(Box::new(e)),
                            Ok(header) => header
                        };
                        println!("read request header {:?}", req_header);

                        let extra_len = req_header.extras_length.into();
                        let key_len = req_header.key_length.into();
                        let total_len = usize::try_from(req_header.total_body_length).unwrap_or(0);
                        let value_len = total_len - extra_len - key_len;

                        let mut extra_buff = vec![0; extra_len];
                        let mut key_buff = vec![0; key_len];
                        let mut value_buff = vec![0; value_len];
                        if extra_len != 0 {
                            stream.read_exact(extra_buff.as_mut_slice()).await?;
                        }
                        if key_len != 0 {
                            stream.read_exact(key_buff.as_mut_slice()).await?;
                        }
                        if value_len != 0 {
                            stream.read_exact(value_buff.as_mut_slice()).await?;
                        }

                        let mut res_header = ResponseHeader {
                            magic: MagicByte::Response.into(),
                            opcode: req_header.opcode,
                            key_length: 0,
                            extras_length: 0,
                            data_type: 0,
                            status: 0,
                            total_body_length: 0,
                            opaque: req_header.opaque,
                            cas: 0,
                        };

                        match CommandOpcodes::try_from(req_header.opcode) {
                            Ok(CommandOpcodes::Get) => {
                                let result = {
                                    kvs.lock().unwrap().get(key_buff.as_slice()).cloned()
                                };

                                match result {
                                    Some(value) => {
                                        res_header.status = ResponseStatus::NoError.into();
                                        res_header.total_body_length = u32::try_from(value.len()).unwrap_or(0);
                                        write_response_header(&mut stream, &res_header).await?;
                                        stream.write_all(value.as_slice()).await?;
                                    }
                                    None => {
                                        res_header.status = ResponseStatus::KeyNotFound.into();
                                        write_response_header(&mut stream, &res_header).await?;
                                    }
                                };
                            }
                            Ok(CommandOpcodes::Set) => {
                                kvs.lock().unwrap().insert(key_buff, value_buff);
                                res_header.status = ResponseStatus::NoError.into();
                                write_response_header(&mut stream, &res_header).await?;
                            }
                            Ok(CommandOpcodes::Delete) => {
                                let deleted = {
                                    let mut k = kvs.lock()?;
                                    k.remove(key_buff.as_slice())
                                };
                                res_header.status = match deleted {
                                    Some(_) => ResponseStatus::NoError.into(),
                                    None => ResponseStatus::KeyNotFound.into()
                                };
                                write_response_header(&mut stream, &res_header).await?;
                            }
                            Ok(CommandOpcodes::Append) => {
                                let stored = {
                                    let mut k = kvs.lock()?;
                                    match k.get_mut(key_buff.as_slice()) {
                                        Some(current) => {
                                            current.append(&mut value_buff);
                                            true
                                        }
                                        None => false
                                    }
                                };
                                res_header.status = if stored {
                                    ResponseStatus::NoError.into()
                                } else {
                                    ResponseStatus::ItemNotStored.into()
                                };
                                write_response_header(&mut stream, &res_header).await?;
                            }
                            Ok(CommandOpcodes::Prepend) => {
                                let stored = {
                                    let mut k = kvs.lock()?;
                                    match k.get_mut(key_buff.as_slice()) {
                                        Some(current) => {
                                            value_buff.append(current);
                                            true
                                        }
                                        None => false
                                    }
                                };
                                res_header.status = if stored {
                                    ResponseStatus::NoError.into()
                                } else {
                                    ResponseStatus::ItemNotStored.into()
                                };
                                write_response_header(&mut stream, &res_header).await?;
                            }
                            Ok(CommandOpcodes::Replace) => {
                                let stored = {
                                    let mut k = kvs.lock()?;
                                    if k.contains_key(key_buff.as_slice()) {
                                        k.insert(key_buff, value_buff);
                                        true
                                    } else {
                                        false
                                    }
                                };
                                res_header.status = if stored {
                                    ResponseStatus::NoError.into()
                                } else {
                                    ResponseStatus::KeyNotFound.into()
                                };
                                write_response_header(&mut stream, &res_header).await?;
                            }
                            Ok(CommandOpcodes::Add) => {
                                let stored = {
                                    let mut k = kvs.lock()?;
                                    if k.contains_key(key_buff.as_slice()) {
                                        false
                                    } else {
                                        k.insert(key_buff, value_buff);
                                        true
                                    }
                                };
                                res_header.status = if stored {
                                    ResponseStatus::NoError.into()
                                } else {
                                    ResponseStatus::KeyExists.into()
                                };
                                write_response_header(&mut stream, &res_header).await?;
                            }
                            _ => {
                                res_header.status = ResponseStatus::KeyNotFound.into();
                                write_response_header(&mut stream, &res_header).await?;
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

async fn read_request_header(stream: &mut BufStream<TcpStream>) -> Result<RequestHeader, IoError> {
    use tokio_byteorder::AsyncReadBytesExt;

    let req = RequestHeader {
        magic: stream.read_u8().await?,
        opcode: stream.read_u8().await?,
        key_length: stream.read_u16::<BigEndian>().await?,
        extras_length: stream.read_u8().await?,
        data_type: stream.read_u8().await?,
        vbucket_id: stream.read_u16::<BigEndian>().await?,
        total_body_length: stream.read_u32::<BigEndian>().await?,
        opaque: stream.read_u32::<BigEndian>().await?,
        cas: stream.read_u64::<BigEndian>().await?,
    };

    Ok(req)
}

async fn write_response_header(stream: &mut BufStream<TcpStream>, header: &ResponseHeader) -> Result<(), IoError> {
    use tokio_byteorder::AsyncWriteBytesExt;

    stream.write_u8(header.magic).await?;
    stream.write_u8(header.opcode).await?;
    stream.write_u16::<BigEndian>(header.key_length).await?;
    stream.write_u8(header.extras_length).await?;
    stream.write_u8(header.data_type).await?;
    stream.write_u16::<BigEndian>(header.status).await?;
    stream.write_u32::<BigEndian>(header.total_body_length).await?;
    stream.write_u32::<BigEndian>(header.opaque).await?;
    stream.write_u64::<BigEndian>(header.cas).await?;
    println!("write response header {:?}", header);
    Ok(())
}


#[allow(dead_code)]
#[derive(Debug)]
struct RequestHeader {
    magic: u8,
    opcode: u8,
    key_length: u16,
    extras_length: u8,
    data_type: u8,
    vbucket_id: u16,
    total_body_length: u32,
    opaque: u32,
    cas: u64,
}


#[allow(dead_code)]
#[derive(Debug)]
struct ResponseHeader {
    magic: u8,
    opcode: u8,
    key_length: u16,
    extras_length: u8,
    data_type: u8,
    status: u16,
    total_body_length: u32,
    opaque: u32,
    cas: u64,
}

#[derive(Copy, Clone, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum MagicByte {
    Request = 0x80,
    Response = 0x81,
}


#[derive(Copy, Clone, IntoPrimitive, TryFromPrimitive)]
#[repr(u16)]
pub enum ResponseStatus {
    NoError = 0x0000,
    KeyNotFound = 0x0001,
    KeyExists = 0x0002,
    ValueTooLarge = 0x0003,
    InvalidArguments = 0x0004,
    ItemNotStored = 0x0005,
    IncrDecrOnNonNumericValue = 0x0006,
    TheVbucketBelongsToAnotherServer = 0x0007,
    AuthenticationError = 0x0008,
    AuthenticationContinue = 0x0009,
    UnknownCommand = 0x0081,
    OutOfMemory = 0x0082,
    NotSupported = 0x0083,
    InternalError = 0x0084,
    Busy = 0x0085,
    TemporaryFailure = 0x0086,
    Unknown = 0xFF,
}


#[derive(Copy, Clone, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum CommandOpcodes {
    Get = 0x00,
    Set = 0x01,
    Add = 0x02,
    Replace = 0x03,
    Delete = 0x04,
    Increment = 0x05,
    Decrement = 0x06,
    Quit = 0x07,
    Flush = 0x08,
    GetQ = 0x09,
    Noop = 0x0a,
    Version = 0x0b,
    GetK = 0x0c,
    GetKQ = 0x0d,
    Append = 0x0e,
    Prepend = 0x0f,
    Stat = 0x10,
    Unknown = 0xFF,
}
