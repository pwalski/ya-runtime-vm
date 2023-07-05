use anyhow::bail;
use futures::lock::Mutex;
use notify::event::{CreateKind, ModifyKind, DataChange};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{mpsc, Arc};
use std::time::Duration;
use tokio::fs;
use tokio::sync::Notify;
use uuid::Uuid;
use ya_runtime_sdk::runtime_api::deploy::ContainerVolume;
use ya_runtime_sdk::runtime_api::server::RuntimeHandler;
use ya_runtime_sdk::{runtime_api::server, server::Server, Context, ErrorExt, EventEmitter};
use ya_runtime_sdk::{Error, ProcessStatus, RuntimeStatus};

use crate::deploy::Deployment;
use crate::vmrt::{runtime_dir, RuntimeData};
use crate::Runtime;

const FILE_TEST_IMAGE: &str = "self-test.gvmi";
const FILE_TEST_EXECUTABLE: &str = "ya-self-test";

pub(crate) async fn test(
    pci_device_id: Option<String>,
    timeout: Duration,
    cpu_cores: usize,
    mem_gib: f64,
) -> Result<(), Error> {
    run_self_test(verify_status, pci_device_id, timeout, cpu_cores, mem_gib).await;
    // Dead code. ya_runtime_api::server::run_async requires killing a process to stop
    Ok(())
}

pub(crate) fn verify_status(status: anyhow::Result<Value>) -> anyhow::Result<String> {
    let status = status?;
    Ok(serde_json::to_string(&status)?)
}

pub(crate) async fn run_self_test<HANDLER>(
    handle_result: HANDLER,
    pci_device_id: Option<String>,
    timeout: Duration,
    cpu_cores: usize,
    mem_gib: f64,
) where
    HANDLER: Fn(anyhow::Result<Value>) -> anyhow::Result<String>,
{
    let work_dir = std::env::temp_dir();

    let deployment = self_test_deployment(&work_dir, cpu_cores, mem_gib)
        .await
        .expect("Prepares self test img deployment");

    let out_volume =
        self_test_only_volume(&deployment).expect("Self test image has an output volume");
    let out_file_name = format!("out_{}.json", Uuid::new_v4());
    let out_file_path_vm = PathBuf::from_str(&out_volume.path)
        .expect("Can create self test volume path")
        .join(&out_file_name);
    let out_dir_path_host = work_dir.join(out_volume.name);
    let out_file_path_host = out_dir_path_host.join(out_file_name);

    let runtime_data = RuntimeData {
        deployment: Some(deployment),
        pci_device_id,
        ..Default::default()
    };
    let runtime = Runtime {
        data: Arc::new(Mutex::new(runtime_data)),
    };

    server::run_async(|e| async {
        let ctx = Context::try_new().expect("Creates runtime context");

        log::info!("Starting runtime");
        let emitter = EventEmitter::spawn(e);
        let start_response = crate::start(work_dir.clone(), runtime.data.clone(), emitter.clone())
            .await
            .expect("Starts runtime");
        log::info!("Runtime start response {:?}", start_response);

        let run_process: ya_runtime_sdk::RunProcess = server::RunProcess {
            bin: format!("/{FILE_TEST_EXECUTABLE}"),
            args: vec![
                FILE_TEST_EXECUTABLE.into(),
                out_file_path_vm.to_string_lossy().into(),
            ],
            work_dir: "/".into(),
            ..Default::default()
        };

        log::info!("Runtime: {:?}", runtime.data);
        log::info!("Self test process: {run_process:?}");

        let _: u64 = crate::run_command(runtime.data.clone(), run_process)
            .await
            .expect("Runs command");

        let output_notification = Arc::new(Notify::new());
        let output_notification_clone = output_notification.clone();
        let mut watcher = notify::recommended_watcher(move |res| match res {
            Ok(Event {
                kind: EventKind::Modify(ModifyKind::Data(DataChange::Any)),
                ..
            }) => output_notification_clone.notify_waiters(),
            Ok(event) => {
                log::debug!("Output file watch event: {:?}", event);
            },
            Err(error) => {
                log::error!("Output file watch error: {:?}", error);
                output_notification_clone.notify_waiters();
            }
        })
        .expect("Can create file watcher");

        watcher
            .watch(&out_dir_path_host, RecursiveMode::Recursive)
            .expect("Can watch self-test output file");

        if let Err(err) = tokio::time::timeout(timeout, output_notification.notified()).await {
            log::error!("Self test job timeout: {err}");
        };

        log::info!("Stopping runtime");
        crate::stop(runtime.data.clone())
            .await
            .expect("Stops runtime");
        
        log::info!("Handling result");
        let out_file = std::fs::File::open(out_file_path_host)
            .expect("File should open read only");
        let out: anyhow::Result<Value> = Ok(serde_json::from_reader(&out_file)
            .expect("File should be a JSON"));
        let result = handle_result(out).expect("Handles test result");
        if !result.is_empty() {
            println!("{result}");
        }

        tokio::spawn(async move {
            // the server refuses to stop by itself; force quit
            std::process::exit(0);
        });

        Server::new(runtime, ctx)
    })
    .await;
}

/// Returns path to self test image only volume.
/// Fails if no volumes or more than one.
fn self_test_only_volume(self_test_deployment: &Deployment) -> anyhow::Result<ContainerVolume> {
    if self_test_deployment.volumes.len() != 1 {
        bail!("Self test image has to have one volume");
    }
    Ok(self_test_deployment.volumes.first().unwrap().clone())
}

async fn self_test_deployment(
    work_dir: &Path,
    cpu_cores: usize,
    mem_gib: f64,
) -> anyhow::Result<Deployment> {
    let package_path = runtime_dir()
        .expect("Runtime directory not found")
        .join(FILE_TEST_IMAGE)
        .canonicalize()
        .expect("Test image not found");

    log::info!("Task package: {}", package_path.display());
    let mem_mib = (mem_gib * 1024.) as usize;
    let package_file = fs::File::open(package_path.clone())
        .await
        .or_err("Error reading package file")?;
    let deployment =
        Deployment::try_from_input(package_file, cpu_cores, mem_mib, package_path.clone())
            .await
            .or_err("Error reading package metadata")?;
    for vol in &deployment.volumes {
        let vol_dir = work_dir.join(&vol.name);
        log::debug!("Creating volume dir: {vol_dir:?} for path {}", vol.path);
        fs::create_dir_all(vol_dir)
            .await
            .or_err("Failed to create volume dir")?;
    }
    Ok(deployment)
}

struct ProcessOutputHandler {
    status_sender: mpsc::Sender<ProcessStatus>,
    handler: Box<dyn RuntimeHandler + 'static>,
}

impl RuntimeHandler for ProcessOutputHandler {
    fn on_process_status<'a>(&self, status: ProcessStatus) -> futures::future::BoxFuture<'a, ()> {
        if let Err(err) = self.status_sender.send(status.clone()) {
            log::warn!("Failed to send process status {err}");
        }
        self.handler.on_process_status(status)
    }

    fn on_runtime_status<'a>(&self, status: RuntimeStatus) -> futures::future::BoxFuture<'a, ()> {
        self.handler.on_runtime_status(status)
    }
}

/// Collects process `stdout` and tries to parse it into `serde_json::Value`.
///  # Arguments
/// * `status_receiver` of `ProcessStatus`
/// * `pid`
/// * `timeout` used to wait for `ProcessStatus`
async fn collect_process_response(
    status_receiver: &mut mpsc::Receiver<ProcessStatus>,
    pid: u64,
    timeout: Duration,
) -> anyhow::Result<Value> {
    log::debug!("Start listening on process: {pid}");
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut return_code = 0;
    while let Ok(status) = status_receiver.recv_timeout(timeout) {
        if status.pid != pid {
            continue;
        }
        stdout.append(&mut status.stdout.clone());
        stderr.append(&mut status.stderr.clone());
        return_code = status.return_code;
        if !status.running {
            // Process stopped
            break;
        } else if status.return_code != 0 {
            // Process failed. Waiting for final message or timeout.
            continue;
        } else if let Ok(response) = serde_json::from_slice(&stdout) {
            // Succeed parsing response.
            return Ok(response);
        }
    }
    if return_code != 0 {
        bail!(String::from_utf8_lossy(&stderr).to_string())
    }
    match serde_json::from_slice(&stdout) {
        Ok(response) => Ok(response),
        Err(err) => {
            if !stderr.is_empty() {
                bail!(String::from_utf8_lossy(&stderr).to_string())
            } else {
                bail!(err)
            }
        }
    }
}
