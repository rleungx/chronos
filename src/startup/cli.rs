use std::env;

use chronos::TsoConfig;

use crate::AppResult;

use super::config::{load_startup_config, LoadedStartupConfig, StartupMetadata};
use super::preflight::{validate_startup_preflight, MetricsTransport, ValidatedStartupPlan};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliCommand {
    Run,
    Help,
    PrintEnvTemplate,
    PrintEffectiveConfig,
    CheckConfig,
}

pub(crate) async fn run_cli_or_service() -> AppResult<()> {
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
            print_effective_config(&startup, &plan);
            Ok(())
        }
        CliCommand::CheckConfig => {
            let startup = load_startup_config()?;
            let plan = validate_startup_preflight(&startup)?;
            println!("configuration valid");
            print_effective_config(&startup, &plan);
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
        "Chronos\n\nCommands:\n  chronos                    Start the service using environment variables\n  chronos --check-config     Validate startup configuration and exit\n  chronos --print-effective-config\n                             Print the effective validated startup configuration\n  chronos --print-env-template\n                             Print a minimal environment template for local runs\n  chronos --help             Show this help\n\nQuick start:\n  Local memory metadata:\n    export CHRONOS_SECURITY_MODE=dev-insecure\n    export CHRONOS_BIND_ADDR=127.0.0.1:50051\n    export CHRONOS_ADVERTISE_ENDPOINT=127.0.0.1:50051\n    cargo run --bin chronos\n\n  Validate config without starting:\n    cargo run --bin chronos -- --check-config\n\n  Inspect what Chronos will use:\n    cargo run --bin chronos -- --print-effective-config\n"
    );
}

fn print_env_template() {
    println!(
        "# Minimal local memory-backed startup\nexport CHRONOS_SECURITY_MODE=dev-insecure\nexport CHRONOS_BIND_ADDR=127.0.0.1:50051\nexport CHRONOS_ADVERTISE_ENDPOINT=127.0.0.1:50051\n\n# Optional identity\n# export CHRONOS_WORKER_ID=worker-a\n# export CHRONOS_INSTANCE_ID=instance-a\n\n# To switch to etcd-backed metadata, also set:\n# export CHRONOS_METADATA=etcd\n# export CHRONOS_ETCD_ENDPOINTS=127.0.0.1:2379\n# export CHRONOS_ETCD_PREFIX=/chronos-local\n# export CHRONOS_WORKER_ID=worker-a\n# export CHRONOS_SAFETY_GAP_MS=1\n"
    );
}

fn print_effective_config(startup: &LoadedStartupConfig, plan: &ValidatedStartupPlan<'_>) {
    println!("build_version={}", chronos::build_version());
    println!("build_commit={}", chronos::build_commit());
    println!("metadata_kind={}", startup.metadata_kind());
    println!("security_mode={}", plan.effective_security_mode());
    println!(
        "metrics_transport={}",
        metrics_transport_label(plan.metrics_transport())
    );
    print_config_lines(&startup.config);
    match &startup.metadata {
        StartupMetadata::Memory => println!("metadata_backend=memory"),
        StartupMetadata::Etcd(etcd) => {
            println!("metadata_backend=etcd");
            println!("etcd_endpoints={}", etcd.endpoints.join(","));
            println!("etcd_prefix={}", etcd.prefix);
        }
    }
}

fn print_config_lines(config: &TsoConfig) {
    println!("worker_id={}", config.worker_id);
    println!("instance_id={}", config.effective_instance_id());
    println!("advertise_endpoint={}", config.advertise_endpoint);
    println!("bind_addr={}", config.bind_addr);
    println!("metrics_bind_addr={}", config.metrics_bind_addr);
    println!("production_profile={}", config.production_profile);
    println!("default_resource_tier={}", config.default_resource_tier);
    println!("shared_generators={}", config.shared_generators);
    println!("warm_generators={}", config.warm_generators);
    println!("max_batch_per_request={}", config.max_batch_per_request);
    println!("lease_ttl_ms={}", config.lease_ttl_ms);
    println!(
        "generator_maintenance_interval_ms={}",
        config.generator_maintenance_interval_ms
    );
    println!("safety_gap_ms={}", config.safety_gap_ms);
    println!(
        "max_timeline_proxy_lanes={}",
        config.max_timeline_proxy_lanes
    );
    println!(
        "max_timeline_runtime_entries={}",
        config.max_timeline_runtime_entries
    );
    println!(
        "generator_ownership={}/{}",
        config.generator_ownership_remainder, config.generator_ownership_modulo
    );
}

fn metrics_transport_label(transport: MetricsTransport) -> &'static str {
    match transport {
        MetricsTransport::Plain => "plain",
        MetricsTransport::Mtls => "mtls",
    }
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
}
