pub mod redis;
pub mod resp;

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use redis::{redis_run, RedisCommand};
use resp::{parse_resp, RespType};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    #[arg(short, long, num_args = 2)]
    replicaof: Option<Vec<String>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd_args = Arc::new(Args::parse());

    let port = cmd_args.port;
    let addr = format!("127.0.0.1:{port}");

    let (tx, rx) = mpsc::channel(100);
    tokio::spawn(redis_run(rx));

    let listener = TcpListener::bind(addr).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let tx = tx.clone();
        let cmd_args = cmd_args.clone();
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => 0,
                };
                if n > 0 {
                    let (a, _) = parse_resp(&buf[0..n]).unwrap();
                    let response = if let Ok((command, args)) = extract_command(a) {
                        if let Some(command) = RedisCommandType::from(&command) {
                            match command {
                                RedisCommandType::Ping => {
                                    RespType::SimpleStrings("PONG".to_string())
                                }
                                RedisCommandType::Echo => args.first().unwrap().clone(),
                                RedisCommandType::Info => {
                                    if args.is_empty() {
                                        unimplemented!();
                                    }
                                    if cmd_args.replicaof.is_some() {
                                        RespType::BulkStrings(b"role:slave".to_vec())
                                    } else {
                                        RespType::BulkStrings(b"role:master".to_vec())
                                    }
                                }
                                RedisCommandType::Set => {
                                    let (reply_tx, mut reply_rx) = mpsc::channel(1);
                                    let _ = tx.send((RedisCommand::Set(args), reply_tx)).await;
                                    if let Some(response) = reply_rx.recv().await {
                                        assert_eq!(response.0.len(), 1);
                                        response.0[0].clone()
                                    } else {
                                        unreachable!();
                                    }
                                }
                                RedisCommandType::Get => {
                                    let (reply_tx, mut reply_rx) = mpsc::channel(1);
                                    let _ = tx
                                        .send((RedisCommand::Get(args[0].clone()), reply_tx))
                                        .await;
                                    if let Some(response) = reply_rx.recv().await {
                                        assert_eq!(response.0.len(), 1);
                                        response.0[0].clone()
                                    } else {
                                        unreachable!();
                                    }
                                }
                            }
                        } else {
                            unimplemented!();
                        }
                    } else {
                        unimplemented!();
                    };
                    let _ = socket.write(format!("{response}").as_bytes()).await;
                }
            }
        });
    }
    //Ok(())
}

fn extract_command(resp: RespType) -> Result<(Vec<u8>, Vec<RespType>)> {
    match resp {
        RespType::Arrays(args) => {
            let command = args.first().context("Arrays(x) is empty?")?;
            if let RespType::BulkStrings(command) = command {
                Ok((command.to_owned(), args.into_iter().skip(1).collect()))
            } else {
                unimplemented!();
            }
        }
        _ => unimplemented!(),
    }
}

enum RedisCommandType {
    Ping,
    Echo,
    Info,
    Set,
    Get,
}

impl RedisCommandType {
    fn from(str: &[u8]) -> Option<Self> {
        match str {
            str if str.eq_ignore_ascii_case(b"ping") => Some(RedisCommandType::Ping),
            str if str.eq_ignore_ascii_case(b"echo") => Some(RedisCommandType::Echo),
            str if str.eq_ignore_ascii_case(b"info") => Some(RedisCommandType::Info),
            str if str.eq_ignore_ascii_case(b"set") => Some(RedisCommandType::Set),
            str if str.eq_ignore_ascii_case(b"get") => Some(RedisCommandType::Get),
            _ => None,
        }
    }
}
