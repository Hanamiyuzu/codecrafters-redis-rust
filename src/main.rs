pub mod resp;

use anyhow::{Context, Result};
use resp::{parse_resp, RespType};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener, process::Command,
};

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
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
                            command if command.eq_ignore_ascii_case(b"ping") => RespType::SimpleStrings(b"PONG"),
                            command if command.eq_ignore_ascii_case(b"echo") => args.first().unwrap().clone(),
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

fn extract_command(resp: RespType) -> Result<(&[u8], Vec<RespType>)> {
    match resp {
        RespType::Arrays(x) => {
            let command = x.first().context("Arrays(x) is empty?")?;
            if let RespType::BulkStrings(command) = command {
                Ok((command, x.into_iter().skip(1).collect()))
            } else {
                unimplemented!();
            }
        }
        _ => unimplemented!(),
    }
}
