pub mod command;
pub mod resp;

use anyhow::{Context, Result};
use command::{redis_run, RedisCommand};
use resp::{parse_resp, RespType};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

#[tokio::main]
async fn main() -> Result<()> {
    let (tx, rx) = mpsc::channel(100);
    tokio::spawn(redis_run(rx));

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let tx = tx.clone();
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
                        match command {
                            command if command.eq_ignore_ascii_case(b"ping") => {
                                RespType::SimpleStrings("PONG".to_string())
                            }
                            command if command.eq_ignore_ascii_case(b"echo") => {
                                args.first().unwrap().clone()
                            }
                            command if command.eq_ignore_ascii_case(b"set") => {
                                let (reply_tx, mut reply_rx) = mpsc::channel(1);
                                let _ = tx.send((RedisCommand::Set(args), reply_tx)).await;
                                if let Some(response) = reply_rx.recv().await {
                                    assert_eq!(response.0.len(), 1);
                                    response.0[0].clone()
                                } else {
                                    unreachable!();
                                }
                            }
                            command if command.eq_ignore_ascii_case(b"get") => {
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
                            _ => unreachable!(),
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
