use chronos::{Client, ClientConfig, ClientTransportConfig};

type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> AppResult<()> {
    let config = ClientConfig::new("orders.primary")
        .with_transport(ClientTransportConfig::default().with_insecure(true));
    let client = Client::connect_with_config("127.0.0.1:50051", config).await?;
    let ranges = client.allocate_timestamps(1).await?;

    println!("tso={}", ranges[0].start_tso);
    Ok(())
}
