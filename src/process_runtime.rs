use std::io;

use tokio::runtime::{Builder, Runtime};

fn invalid_worker_threads(env_key: &str, value: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{env_key} must be a positive integer when set, got: {value}"),
    )
}

pub fn configured_worker_threads(env_key: &str) -> io::Result<Option<usize>> {
    let Some(value) = std::env::var_os(env_key) else {
        return Ok(None);
    };
    let value = value.to_string_lossy();
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let worker_threads = value
        .parse::<usize>()
        .map_err(|_| invalid_worker_threads(env_key, value))?;
    if worker_threads == 0 {
        return Err(invalid_worker_threads(env_key, value));
    }
    Ok(Some(worker_threads))
}

pub fn build_multi_thread_runtime(env_key: &str, thread_name: &str) -> io::Result<Runtime> {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all().thread_name(thread_name);
    if let Some(worker_threads) = configured_worker_threads(env_key)? {
        builder.worker_threads(worker_threads);
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EnvGuard(&'static str);

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                std::env::remove_var(self.0);
            }
        }
    }

    fn set_test_env(key: &'static str, value: &str) -> EnvGuard {
        unsafe {
            std::env::set_var(key, value);
        }
        EnvGuard(key)
    }

    #[test]
    fn configured_worker_threads_accepts_positive_value() {
        const TEST_ENV: &str = "CHRONOS_TEST_RUNTIME_WORKER_THREADS_ACCEPTS";
        let _guard = set_test_env(TEST_ENV, "2");

        assert_eq!(configured_worker_threads(TEST_ENV).unwrap(), Some(2));
    }

    #[test]
    fn configured_worker_threads_treats_empty_as_default() {
        const TEST_ENV: &str = "CHRONOS_TEST_RUNTIME_WORKER_THREADS_EMPTY";
        let _guard = set_test_env(TEST_ENV, " ");

        assert_eq!(configured_worker_threads(TEST_ENV).unwrap(), None);
    }

    #[test]
    fn configured_worker_threads_rejects_zero() {
        const TEST_ENV: &str = "CHRONOS_TEST_RUNTIME_WORKER_THREADS_ZERO";
        let _guard = set_test_env(TEST_ENV, "0");

        assert!(configured_worker_threads(TEST_ENV).is_err());
    }
}
