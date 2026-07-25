use std::env;

use chronos::{metadata::identity_lease_grant_request_ttl_seconds, TsoConfig};
use tokio::time::Duration;

use crate::AppResult;

use super::config::{load_startup_config, LoadedStartupConfig, StartupMetadata};
use super::preflight::{validate_startup_preflight, MetricsTransport, ValidatedStartupPlan};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliCommand {
    Run,
    Help,
    PrintEnvTemplate,
    PrintEffectiveConfig,
    PrintOwnershipEnv,
    CheckConfig,
}

pub(crate) async fn run_cli_or_service() -> AppResult<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    match parse_cli_command(env::args().skip(1))? {
        CliCommand::Run => super::runtime::run().await,
        CliCommand::Help => {
            print_help();
            Ok(())
        }
        CliCommand::PrintEnvTemplate => {
            print_env_template();
            Ok(())
        }
        CliCommand::PrintEffectiveConfig => {
            let startup = load_startup_config()?;
            let plan = validate_startup_preflight(&startup)?;
            print_effective_config(&startup, &plan)?;
            Ok(())
        }
        CliCommand::PrintOwnershipEnv => {
            let ownership = ownership_env_from_process_env()?;
            print_ownership_env(&ownership);
            Ok(())
        }
        CliCommand::CheckConfig => {
            let startup = load_startup_config()?;
            let plan = validate_startup_preflight(&startup)?;
            println!("configuration valid");
            print_effective_config(&startup, &plan)?;
            Ok(())
        }
    }
}

fn parse_cli_command<I>(args: I) -> AppResult<CliCommand>
where
    I: IntoIterator<Item = String>,
{
    let args = args.into_iter().collect::<Vec<_>>();
    match args.as_slice() {
        [] => Ok(CliCommand::Run),
        [arg] if matches!(arg.as_str(), "-h" | "--help" | "help") => Ok(CliCommand::Help),
        [arg] if arg == "--print-env-template" => Ok(CliCommand::PrintEnvTemplate),
        [arg] if arg == "--print-effective-config" => Ok(CliCommand::PrintEffectiveConfig),
        [arg] if arg == "--print-ownership-env" => Ok(CliCommand::PrintOwnershipEnv),
        [arg] if arg == "--check-config" => Ok(CliCommand::CheckConfig),
        _ => Err(format!(
            "unsupported arguments: {}\nrun `chronos --help` to see supported commands",
            args.join(" ")
        )
        .into()),
    }
}

fn print_help() {
    println!(
        "Chronos\n\nCommands:\n  chronos                    Start the service using environment variables\n  chronos --check-config     Validate startup configuration and exit\n  chronos --print-effective-config\n                             Print the effective validated startup configuration\n  chronos --print-env-template\n                             Print a minimal environment template for local runs\n  chronos --print-ownership-env\n                             Print StatefulSet ownership env exports for the current pod\n  chronos --help             Show this help\n\nQuick start:\n  Local memory metadata:\n    export CHRONOS_SECURITY_MODE=dev-insecure\n    export CHRONOS_BIND_ADDR=127.0.0.1:50051\n    export CHRONOS_ADVERTISE_ENDPOINT=127.0.0.1:50051\n    export CHRONOS_LOG_FORMAT=json\n    export CHRONOS_LOG_FILTER=info\n    cargo run --bin chronos\n\n  Validate config without starting:\n    cargo run --bin chronos -- --check-config\n\n  Inspect what Chronos will use:\n    cargo run --bin chronos -- --print-effective-config\n"
    );
}

fn print_env_template() {
    println!(
        r#"# Minimal local memory-backed startup
export CHRONOS_SECURITY_MODE=dev-insecure
export CHRONOS_BIND_ADDR=127.0.0.1:50051
export CHRONOS_ADVERTISE_ENDPOINT=127.0.0.1:50051
export CHRONOS_LOG_FORMAT=json
export CHRONOS_LOG_FILTER=info

# Optional identity
# export CHRONOS_WORKER_ID=worker-a
# export CHRONOS_INSTANCE_ID=instance-a

# To switch to etcd-backed metadata, also set:
# export CHRONOS_METADATA=etcd
# export CHRONOS_ETCD_ENDPOINTS=127.0.0.1:2379
# export CHRONOS_ETCD_PREFIX=/chronos-local
# export CHRONOS_WORKER_ID=worker-a
# export CHRONOS_MAX_CLOCK_SKEW_MS=500
# export CHRONOS_SAFETY_GAP_MS=500

# Optional multi-node generator partitioning:
# export CHRONOS_OWNERSHIP_PLAN_ID=scale-2026-05-10-a
# export CHRONOS_GENERATOR_OWNERSHIP_MODULO=2
# export CHRONOS_GENERATOR_OWNERSHIP_REMAINDER=0"#
    );
}

fn print_effective_config(
    startup: &LoadedStartupConfig,
    plan: &ValidatedStartupPlan<'_>,
) -> AppResult<()> {
    println!("build_version={}", chronos::build_version());
    println!("build_commit={}", chronos::build_commit());
    println!("metadata_kind={}", startup.metadata_kind());
    println!("security_mode={}", plan.effective_security_mode());
    println!("log_format={}", startup.logging.format);
    println!("log_filter={}", startup.logging.filter);
    println!(
        "metrics_transport={}",
        metrics_transport_label(plan.metrics_transport())
    );
    print_config_lines(&startup.config)?;
    match &startup.metadata {
        StartupMetadata::Memory => println!("metadata_backend=memory"),
        StartupMetadata::Etcd(etcd) => {
            println!("metadata_backend=etcd");
            println!("etcd_endpoints={}", etcd.endpoints.join(","));
            println!("etcd_prefix={}", etcd.prefix);
        }
    }
    Ok(())
}

fn print_config_lines(config: &TsoConfig) -> AppResult<()> {
    println!("worker_id={}", config.worker_id);
    println!("instance_id={}", config.effective_instance_id());
    println!("advertise_endpoint={}", config.advertise_endpoint);
    println!("bind_addr={}", config.bind_addr);
    println!(
        "health_bind_addr={}",
        config.health_bind_addr.as_deref().unwrap_or("disabled")
    );
    println!("metrics_bind_addr={}", config.metrics_bind_addr);
    println!("production_profile={}", config.production_profile);
    println!("ownership_plan_id={}", config.ownership_plan_id);
    println!(
        "generator_ownership_modulo={}",
        config.generator_ownership_modulo
    );
    println!(
        "generator_ownership_remainder={}",
        config.generator_ownership_remainder
    );
    let remainders = config
        .effective_generator_ownership_remainders()
        .into_iter()
        .map(|remainder| remainder.to_string())
        .collect::<Vec<_>>()
        .join(",");
    println!("generator_ownership_remainders={}", remainders);
    println!("safety_gap_ms={}", config.safety_gap_ms);
    println!("max_clock_skew_ms={}", config.max_clock_skew_ms);
    let identity_ttl = Duration::from_millis(config.lease_ttl_ms);
    let identity_grant_request_seconds = identity_lease_grant_request_ttl_seconds(identity_ttl)?;
    let identity_grant_request_ms = u128::try_from(identity_grant_request_seconds)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000))
        .ok_or("identity lease grant request TTL milliseconds overflowed u128")?;
    println!("identity_lease_ttl_configured_ms={}", config.lease_ttl_ms);
    println!(
        "identity_lease_grant_request_ttl_seconds={}",
        identity_grant_request_seconds
    );
    println!(
        "identity_lease_grant_request_ttl_ms={}",
        identity_grant_request_ms
    );
    let (generator_maintenance_effective_ms, generator_maintenance_reason) =
        config.effective_generator_maintenance_cadence();
    println!(
        "generator_lease_ttl_configured_ms={}",
        config.generator_lease_ttl_ms
    );
    println!(
        "generator_lease_ttl_effective_ms={}",
        config.effective_generator_lease_ttl_ms()
    );
    println!(
        "generator_maintenance_interval_configured_ms={}",
        config.generator_maintenance_interval_ms
    );
    println!("generator_maintenance_interval_effective_ms={generator_maintenance_effective_ms}");
    println!("generator_issued_horizon_ms={}", config.pre_borrow_ms);
    println!("generator_maintenance_cadence_reason={generator_maintenance_reason}");
    println!("auto_failover_enabled={}", config.auto_failover_enabled);
    println!(
        "auto_failover_interval_ms={}",
        config.auto_failover_interval_ms
    );
    println!(
        "auto_failover_batch_size={}",
        config.auto_failover_batch_size
    );
    println!("max_timeline_records={}", config.max_timeline_records);
    println!("grpc_max_connections={}", config.grpc_max_connections);
    println!(
        "cluster_format_version={}",
        chronos::metadata::CURRENT_CLUSTER_FORMAT_VERSION
    );
    println!("tso_max_supported_unix_ms={}", chronos::MAX_UNIX_MS);
    Ok(())
}

fn metrics_transport_label(transport: MetricsTransport) -> &'static str {
    match transport {
        MetricsTransport::Plain => "plain",
        MetricsTransport::Mtls => "mtls",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnershipEnv {
    primary_remainder: u32,
    remainders: Vec<u32>,
}

fn ownership_env_from_process_env() -> AppResult<OwnershipEnv> {
    let pod_name = env::var("POD_NAME")?;
    let ordinal = statefulset_ordinal(&pod_name)?;
    let worker_count = parse_required_u32_env("CHRONOS_OWNERSHIP_WORKER_COUNT")?;
    let shard_count = parse_required_u32_env("CHRONOS_GENERATOR_OWNERSHIP_MODULO")?;
    let assignment_seed = parse_optional_u64_env("CHRONOS_OWNERSHIP_ASSIGNMENT_SEED", 0)?;
    ownership_env_for_ordinal(ordinal, worker_count, shard_count, assignment_seed)
}

fn parse_required_u32_env(key: &str) -> AppResult<u32> {
    let value = env::var(key)?;
    parse_u32_value(key, &value)
}

fn parse_optional_u64_env(key: &str, default: u64) -> AppResult<u64> {
    match env::var(key) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|error| format!("{key} must be numeric, got {value}: {error}").into()),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn parse_u32_value(name: &str, value: &str) -> AppResult<u32> {
    value
        .parse::<u32>()
        .map_err(|error| format!("{name} must be numeric, got {value}: {error}").into())
}

fn statefulset_ordinal(pod_name: &str) -> AppResult<u32> {
    let (_, ordinal) = pod_name.rsplit_once('-').ok_or_else(|| {
        format!("POD_NAME must end with a numeric StatefulSet ordinal, got: {pod_name}")
    })?;
    parse_u32_value("POD_NAME ordinal", ordinal)
}

fn ownership_env_for_ordinal(
    ordinal: u32,
    worker_count: u32,
    shard_count: u32,
    assignment_seed: u64,
) -> AppResult<OwnershipEnv> {
    if shard_count == 0 {
        return Err("CHRONOS_GENERATOR_OWNERSHIP_MODULO must be > 0".into());
    }
    if worker_count == 0 {
        return Err("CHRONOS_OWNERSHIP_WORKER_COUNT must be > 0".into());
    }
    if ordinal >= worker_count {
        return Err(format!(
            "pod ordinal {ordinal} must be < CHRONOS_OWNERSHIP_WORKER_COUNT={worker_count}"
        )
        .into());
    }

    let remainders = (0..shard_count)
        .filter(|shard| owner_for_shard(worker_count, *shard, assignment_seed) == ordinal)
        .collect::<Vec<_>>();
    let Some(primary_remainder) = remainders.first().copied() else {
        return Err(format!(
            "pod ordinal {ordinal} owns no generator shards; increase ownership shard count or reduce replicas"
        )
        .into());
    };

    Ok(OwnershipEnv {
        primary_remainder,
        remainders,
    })
}

fn owner_for_shard(worker_count: u32, shard: u32, assignment_seed: u64) -> u32 {
    let mut best_worker = 0;
    let mut best_score = 0;
    for worker in 0..worker_count {
        let score = ownership_score(shard as u64, worker as u64, assignment_seed);
        if worker == 0 || score > best_score {
            best_score = score;
            best_worker = worker;
        }
    }
    best_worker
}

fn ownership_score(shard: u64, worker: u64, assignment_seed: u64) -> u64 {
    const MODULUS: u64 = 2_147_483_647;
    const MULTIPLIER: u64 = 1_103_515_245;
    const INCREMENT: u64 = 12_345;
    const WORKER_SALT: u64 = 97;

    let mut mixed = ((((shard + 1) * MULTIPLIER) % MODULUS)
        + (((worker + 1) * INCREMENT) % MODULUS)
        + (assignment_seed % MODULUS))
        % MODULUS;
    mixed = (mixed ^ (mixed >> 16)) & MODULUS;
    mixed = ((mixed * MULTIPLIER) + INCREMENT) % MODULUS;
    mixed = (mixed ^ (mixed >> 11)) & MODULUS;
    ((mixed * MULTIPLIER) + INCREMENT + (worker * WORKER_SALT)) % MODULUS
}

fn print_ownership_env(ownership: &OwnershipEnv) {
    let remainders = ownership
        .remainders
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "export CHRONOS_GENERATOR_OWNERSHIP_REMAINDER='{}'",
        ownership.primary_remainder
    );
    println!("export CHRONOS_GENERATOR_OWNERSHIP_REMAINDERS='{remainders}'");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cli_command_defaults_to_run() {
        assert_eq!(
            parse_cli_command(Vec::<String>::new()).unwrap(),
            CliCommand::Run
        );
    }

    #[test]
    fn parse_cli_command_accepts_help_aliases() {
        for arg in ["-h", "--help", "help"] {
            assert_eq!(
                parse_cli_command(vec![arg.to_string()]).unwrap(),
                CliCommand::Help
            );
        }
    }

    #[test]
    fn parse_cli_command_rejects_unknown_arguments() {
        let error = parse_cli_command(vec!["--wat".to_string()]).unwrap_err();
        assert!(error.to_string().contains("chronos --help"));
    }

    #[test]
    fn parse_cli_command_accepts_ownership_env_command() {
        assert_eq!(
            parse_cli_command(vec!["--print-ownership-env".to_string()]).unwrap(),
            CliCommand::PrintOwnershipEnv
        );
    }

    #[test]
    fn ownership_env_assigns_every_shard_once() {
        let mut seen = Vec::new();
        for ordinal in 0..3 {
            let env = ownership_env_for_ordinal(ordinal, 3, 8, 20_260_516).unwrap();
            assert_eq!(env.primary_remainder, env.remainders[0]);
            seen.extend(env.remainders);
        }
        seen.sort_unstable();
        assert_eq!(seen, (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn ownership_env_matches_rollout_plan_hashing() {
        assert_eq!(
            ownership_env_for_ordinal(0, 3, 8, 20_260_516).unwrap(),
            OwnershipEnv {
                primary_remainder: 3,
                remainders: vec![3, 7],
            }
        );
        assert_eq!(
            ownership_env_for_ordinal(1, 3, 8, 20_260_516).unwrap(),
            OwnershipEnv {
                primary_remainder: 2,
                remainders: vec![2, 4, 6],
            }
        );
        assert_eq!(
            ownership_env_for_ordinal(2, 3, 8, 20_260_516).unwrap(),
            OwnershipEnv {
                primary_remainder: 0,
                remainders: vec![0, 1, 5],
            }
        );
    }

    #[test]
    fn ownership_env_rejects_out_of_range_ordinal() {
        let error = ownership_env_for_ordinal(3, 3, 8, 0).unwrap_err();
        assert!(error.to_string().contains("pod ordinal 3"));
    }

    #[test]
    fn startup_log_format_parses_known_values() {
        assert_eq!(
            "json"
                .parse::<crate::startup::config::StartupLogFormat>()
                .unwrap(),
            crate::startup::config::StartupLogFormat::Json
        );
        assert_eq!(
            "text"
                .parse::<crate::startup::config::StartupLogFormat>()
                .unwrap(),
            crate::startup::config::StartupLogFormat::Text
        );
    }
}
