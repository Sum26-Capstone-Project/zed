use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::Context as _;
use gpui::BackgroundExecutor;

const HEALTH_ADDRESS: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8765);
const SERVER_LOG_PATH: &str = "/tmp/realtime-stt.log";
const SERVER_START_TIMEOUT: Duration = Duration::from_secs(120);
const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(500);
const HEALTH_CONNECT_TIMEOUT: Duration = Duration::from_millis(200);

pub async fn ensure_running(executor: &BackgroundExecutor) -> anyhow::Result<()> {
    if is_healthy() {
        return Ok(());
    }

    spawn_server().context("failed to start speech-to-text server")?;
    wait_for_healthy(executor).await
}

async fn wait_for_healthy(executor: &BackgroundExecutor) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + SERVER_START_TIMEOUT;
    while std::time::Instant::now() < deadline {
        if is_healthy() {
            log::info!("speech-to-text server is ready");
            return Ok(());
        }
        executor.timer(HEALTH_POLL_INTERVAL).await;
    }

    anyhow::bail!(
        "timed out waiting for speech-to-text server on {HEALTH_ADDRESS}; check {SERVER_LOG_PATH}"
    )
}

fn is_healthy() -> bool {
    TcpStream::connect_timeout(&HEALTH_ADDRESS, HEALTH_CONNECT_TIMEOUT).is_ok()
}

fn spawn_server() -> anyhow::Result<()> {
    let server_dir = locate_server_dir().with_context(|| {
        "could not find realtime-stt server directory; install the realtime-stt extension or set REALTIME_STT_SERVER_DIR"
    })?;
    let setup_script = server_dir.join("setup.sh");
    anyhow::ensure!(
        setup_script.is_file(),
        "realtime-stt setup script not found at {}",
        setup_script.display()
    );

    log::info!(
        "starting speech-to-text server from {}, logs: {SERVER_LOG_PATH}",
        server_dir.display()
    );

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(SERVER_LOG_PATH)
        .with_context(|| format!("failed to open {SERVER_LOG_PATH}"))?;

    Command::new("bash")
        .current_dir(&server_dir)
        .env("VENV_DIR", server_dir.join(".venv"))
        .env("SETTINGS_PATH", server_dir.join("settings.yaml"))
        .arg("setup.sh")
        .stdout(log_file.try_clone()?)
        .stderr(log_file)
        .spawn()
        .context("failed to spawn speech-to-text server process")?;

    Ok(())
}

fn locate_server_dir() -> Option<PathBuf> {
    if let Ok(server_dir) = std::env::var("REALTIME_STT_SERVER_DIR") {
        let path = PathBuf::from(server_dir);
        if path.join("setup.sh").is_file() {
            return Some(path);
        }
    }

    let installed_server = paths::data_dir()
        .join("extensions")
        .join("installed")
        .join("realtime-stt")
        .join("server");
    if installed_server.join("setup.sh").is_file() {
        return Some(installed_server);
    }

    if let Ok(current_dir) = std::env::current_dir() {
        if let Some(server_dir) = find_server_dir_in_parents(current_dir) {
            return Some(server_dir);
        }
    }

    None
}

fn find_server_dir_in_parents(mut directory: PathBuf) -> Option<PathBuf> {
    loop {
        let candidate = directory.join("realtime-stt-zed").join("server");
        if candidate.join("setup.sh").is_file() {
            return Some(candidate);
        }

        if !directory.pop() {
            return None;
        }
    }
}
