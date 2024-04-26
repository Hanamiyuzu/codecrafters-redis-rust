use anyhow::{Ok, Result};
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        socket.write(b"+PONG\r\n+PONG\r\n").await?;
    }
    //Ok(())
}
