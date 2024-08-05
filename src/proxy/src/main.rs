use crate::config_parser::ProxyConfig;
use crate::connections::{PlainTextPartitionFinder, TlsPartitionFinder};
use crate::tls::TlsConfig;
use clap::Parser;
use controller::Controller;
use libc::{c_char, c_void};
use log::{debug, error, info};
use std::path::Path;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::signal;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use jemallocator::Jemalloc;
use std::ffi::CStr;

mod config_parser;
mod connections;
mod controller;
mod efs_rpc;
mod error;
mod logger;
mod proxy;
mod proxy_identifier;
mod rpc;
mod shutdown;
mod status_reporter;
mod tls;

#[allow(clippy::all)]
#[allow(deprecated)]
#[allow(invalid_value)]
#[allow(non_camel_case_types)]
#[allow(unused_assignments)]
mod efs_prot {
    include!(concat!(env!("OUT_DIR"), "/efs_prot_xdr.rs"));
}

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// NB: idea stolen from S3BlobAssemblerRust which is subsequently stolen from:
// https://github.com/gnzlbg/jemallocator/blob/master/jemalloc-sys/tests/malloc_conf_set.rs#L14
// This is unsafe but acceptable since issues would be caught at the beginning of the program
union U {
    x: &'static u8,
    y: &'static libc::c_char,
}

#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: Option<&'static libc::c_char> = Some(unsafe {
    U {
        x: &b"narenas:1,tcache_max:1024,dirty_decay_ms:0,muzzy_decay_ms:1000\0"[0],
    }
    .y
});

// We define our own function because letting jemalloc print directly makes a mess, especially in
// in unit testing
unsafe extern "C" fn stats_to_string(buf: *mut c_void, stats_str: *const c_char) {
    let s = CStr::from_ptr(stats_str)
        .to_str()
        .unwrap_or("Could not parse stats");
    let buf = &mut *(buf as *mut String);
    buf.push_str(s);
}

// Doc mentions that we should avoid allocations in the callback
// I've seen in some cases that this can be *really large* (especially on Graviton)
// Decrease this value if we decide to print less
const JEMALLOC_STATS_BUF_LEN: usize = 2 * 1024 * 1024;

pub fn jemalloc_stats(opts: &[c_char]) -> String {
    let mut buf = String::with_capacity(JEMALLOC_STATS_BUF_LEN);
    let buf_ref = (&mut buf) as *mut _ as *mut c_void;
    unsafe {
        jemalloc_sys::malloc_stats_print(Some(stats_to_string), buf_ref, opts.as_ptr());
    }
    buf
}

#[allow(clippy::unnecessary_cast)]
pub fn jemalloc_get_config() -> String {
    // c_char is not defined the same on aarch64 (u8) than on al2/al2012/mac (i8)
    // so on aarch64 the explicit casting is useless. Since it does not hurt, I prefer to keep it
    let opts = [
        b'm' as c_char,
        b'a' as c_char,
        b'b' as c_char,
        b'l' as c_char,
        b'x' as c_char,
        0,
    ];
    jemalloc_stats(&opts)
}

pub fn jemalloc_get_stats() -> String {
    let opts = [0];
    jemalloc_stats(&opts)
}


#[tokio::main]
async fn main() {
    let args = Args::parse();
    jemalloc_get_stats();

    let proxy_config = match ProxyConfig::from_path(Path::new(&args.proxy_config_path)) {
        Ok(config) => config,
        Err(e) => panic!("Failed to read configuration. {}", e),
    };

    if let Some(_log_file_path) = &proxy_config.output {
        logger::init(&proxy_config)
    }

    info!("Running with configuration: {:?}", proxy_config);

    let pid_file_path = Path::new(&proxy_config.pid_file_path);
    let _ = write_pid_file(pid_file_path).await;

    // This "status reporter" is currently only used in tests
    let (_status_requester, status_reporter) = status_reporter::create_status_channel();

    let sigterm_cancellation_token = CancellationToken::new();
    let mut sigterm_listener = match signal::unix::signal(signal::unix::SignalKind::terminate()) {
        Ok(listener) => listener,
        Err(e) => panic!("Failed to create SIGTERM listener. {}", e),
    };

    let controller_handle = if args.tls {
        let tls_config = match get_tls_config(&proxy_config).await {
            Ok(config) => Arc::new(Mutex::new(config)),
            Err(e) => panic!("Failed to obtain TLS config:{}", e),
        };

        run_sighup_handler(proxy_config.clone(), tls_config.clone());

        let controller = Controller::new(
            &proxy_config.nested_config.listen_addr,
            Arc::new(TlsPartitionFinder::new(tls_config)),
            status_reporter,
        )
        .await;
        tokio::spawn(controller.run(sigterm_cancellation_token.clone()))
    } else {
        let controller = Controller::new(
            &proxy_config.nested_config.listen_addr,
            Arc::new(PlainTextPartitionFinder {
                mount_target_addr: proxy_config.nested_config.mount_target_addr.clone(),
            }),
            status_reporter,
        )
        .await;
        tokio::spawn(controller.run(sigterm_cancellation_token.clone()))
    };

    tokio::select! {
        shutdown_reason = controller_handle => error!("Shutting down. {:?}", shutdown_reason),
        _ = sigterm_listener.recv() => {
            info!("Received SIGTERM");
            sigterm_cancellation_token.cancel();
        },
    }
    if pid_file_path.exists() {
        match tokio::fs::remove_file(&pid_file_path).await {
            Ok(()) => info!("Removed pid file"),
            Err(e) => error!("Unable to remove pid_file: {e}"),
        }
    }
}

async fn write_pid_file(pid_file_path: &Path) -> Result<(), anyhow::Error> {
    let mut pid_file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o644)
        .open(pid_file_path)
        .await?;
    pid_file
        .write_all(std::process::id().to_string().as_bytes())
        .await?;
    pid_file.write_u8(b'\x0A').await?;
    pid_file.flush().await?;
    Ok(())
}

async fn get_tls_config(proxy_config: &ProxyConfig) -> Result<TlsConfig, anyhow::Error> {
    let tls_config = TlsConfig::new(
        proxy_config.fips,
        Path::new(&proxy_config.nested_config.ca_file),
        Path::new(&proxy_config.nested_config.client_cert_pem_file),
        Path::new(&proxy_config.nested_config.client_private_key_pem_file),
        &proxy_config.nested_config.mount_target_addr,
        &proxy_config.nested_config.expected_server_hostname_tls,
    )
    .await;
    let tls_config = tls_config?;
    Ok(tls_config)
}

fn run_sighup_handler(proxy_config: ProxyConfig, tls_config: Arc<Mutex<TlsConfig>>) {
    tokio::spawn(async move {
        let mut sighup_listener = match signal::unix::signal(signal::unix::SignalKind::hangup()) {
            Ok(listener) => listener,
            Err(e) => panic!("Failed to create SIGHUP listener. {}", e),
        };

        loop {
            sighup_listener
                .recv()
                .await
                .expect("SIGHUP listener stream is closed");

            debug!("Received SIGHUP");
            let mut locked_config = tls_config.lock().await;
            match get_tls_config(&proxy_config).await {
                Ok(config) => *locked_config = config,
                Err(e) => panic!("Failed to acquire TLS config. {}", e),
            }
        }
    });
}

#[derive(Parser, Debug, Clone)]
pub struct Args {
    pub proxy_config_path: String,

    #[arg(long, default_value_t = false)]
    pub tls: bool,
}

#[cfg(test)]
pub mod tests {

    use super::*;

    #[tokio::test]
    async fn test_write_pid_file() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pid_file = tempfile::NamedTempFile::new()?;
        let pid_file_path = pid_file.path();

        write_pid_file(pid_file_path).await?;

        let expected_pid = std::process::id().to_string();
        let read_pid = tokio::fs::read_to_string(pid_file_path).await?;
        assert_eq!(expected_pid + "\n", read_pid);
        Ok(())
    }
}
