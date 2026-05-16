use std::error::Error;

type AppResult<T> = Result<T, Box<dyn Error>>;

mod startup;

fn main() -> AppResult<()> {
    chronos::process_runtime::build_multi_thread_runtime(
        "CHRONOS_RUNTIME_WORKER_THREADS",
        "chronos-runtime",
    )?
    .block_on(startup::run_cli_or_service())
}
