use anyhow::bail;
use futures::lock::Mutex;
use notify::event::{DataChange, ModifyKind};
use notify::{Event, EventKind, INotifyWatcher, RecursiveMode, Watcher};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::sync::Notify;
use uuid::Uuid;
use ya_runtime_sdk::runtime_api::deploy::ContainerVolume;
use ya_runtime_sdk::RunProcess;
use ya_runtime_sdk::{runtime_api::server, server::Server, Context, Error, ErrorExt, EventEmitter};

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

    let output_volume =
        self_test_only_volume(&deployment).expect("Self test image has an output volume");
    let output_file_name = format!("out_{}.json", Uuid::new_v4());
    let output_file_vm = PathBuf::from_str(&output_volume.path)
        .expect("Can create self test volume path")
        .join(&output_file_name);
    let output_dir = work_dir.join(output_volume.name);
    let output_file = output_dir.join(output_file_name);

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

        let run_process: RunProcess = server::RunProcess {
            bin: format!("/{FILE_TEST_EXECUTABLE}"),
            args: vec![
                FILE_TEST_EXECUTABLE.into(),
                output_file_vm.to_string_lossy().into(),
            ],
            ..Default::default()
        };

        log::info!("Runtime: {:?}", runtime.data);
        log::info!("Self test process: {run_process:?}");
        run_command(runtime.data.clone(), run_process, &output_dir, timeout)
            .await
            .expect("Can run self-test command");

        log::info!("Stopping runtime");
        crate::stop(runtime.data.clone())
            .await
            .expect("Stops runtime");

        log::info!("Handling result");
        let out_file = std::fs::File::open(output_file).expect("Output file should exist");
        let out: anyhow::Result<Value> =
            Ok(serde_json::from_reader(&out_file).expect("Output file should be a JSON"));
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

/// Returns path to self test image only volume.
/// Fails if no volumes or more than one.
fn self_test_only_volume(self_test_deployment: &Deployment) -> anyhow::Result<ContainerVolume> {
    if self_test_deployment.volumes.len() != 1 {
        bail!("Self test image has to have one volume");
    }
    Ok(self_test_deployment.volumes.first().unwrap().clone())
}

async fn run_command(
    runtime_data: Arc<Mutex<RuntimeData>>,
    run_process: RunProcess,
    output_dir: &Path,
    timeout: Duration,
) -> anyhow::Result<()> {
    let output_notification = Arc::new(Notify::new());
    let _watcher = spawn_output_watcher(output_notification.clone(), &output_dir)?;

    if let Err(err) = crate::run_command(runtime_data, run_process).await {
        bail!("Code: {}, msg: {}", err.code, err.message);
    };

    if let Err(err) = tokio::time::timeout(timeout, output_notification.notified()).await {
        log::error!("Self test job timeout: {err}");
    };
    Ok(())
}

fn spawn_output_watcher(
    output_notification: Arc<Notify>,
    output_dir: &Path,
) -> anyhow::Result<INotifyWatcher> {
    let mut watcher = notify::recommended_watcher(move |res| match res {
        Ok(Event {
            kind: EventKind::Modify(ModifyKind::Data(DataChange::Any)),
            ..
        }) => output_notification.notify_waiters(),
        Ok(event) => {
            log::debug!("Output file watch event: {:?}", event);
        }
        Err(error) => {
            log::error!("Output file watch error: {:?}", error);
            output_notification.notify_waiters();
        }
    })?;

    watcher.watch(&output_dir, RecursiveMode::Recursive)?;
    Ok(watcher)
}
