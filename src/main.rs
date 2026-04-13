use std::error::Error;

type AppResult<T> = Result<T, Box<dyn Error>>;

mod startup;

#[tokio::main]
async fn main() -> AppResult<()> {
    startup::run_cli_or_service().await
}
