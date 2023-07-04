use anyhow::bail;
use futures::FutureExt;
use futures::lock::Mutex;
use serde_json::Value;
use std::path::Path;
use std::process::Stdio;
use std::str::FromStr;
use std::sync::{mpsc, Arc};
use std::time::Duration;
use tokio::{fs, process, spawn};
use ya_runtime_sdk::runtime_api::server::RuntimeHandler;
use ya_runtime_sdk::{runtime_api::server, server::Server, Context, ErrorExt, EventEmitter};
use ya_runtime_sdk::{Error, ProcessStatus, RuntimeStatus};
use crate::guest_agent_comm::{GuestAgent, Notification, RedirectFdType};


use crate::deploy::Deployment;
use crate::vmrt::{runtime_dir, RuntimeData, notification_into_status, FILE_VMLINUZ, FILE_INITRAMFS, FILE_RUNTIME, configure_chardev_endpoint, configure_netdev_endpoint, SocketPairConf, reader_to_log};
use crate::Runtime;

const FILE_TEST_IMAGE: &str = "self-test.gvmi";

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

struct Notifications {
    process_died: tokio::sync::Notify,
    output_available: tokio::sync::Notify,
}

impl Notifications {
    fn new() -> Self {
        Notifications {
            process_died: tokio::sync::Notify::new(),
            output_available: tokio::sync::Notify::new(),
        }
    }

    fn handle(&self, notification: Notification) {
        match notification {
            Notification::OutputAvailable { id, fd } => {
                println!("Process {} has output available on fd {}", id, fd);
                self.output_available.notify_waiters();
            }
            Notification::ProcessDied { id, reason } => {
                println!("Process {} died with {:?}", id, reason);
                self.process_died.notify_waiters();
            }
        }
    }
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
        
        let notifications = Arc::new(Notifications::new());

        let start_response: anyhow::Result<()> = async {
            let work_dir= work_dir.clone();
            let runtime_data=  runtime.data.clone();
            // let emitter: EventEmitter= emitter.clone();

            let runtime_dir = runtime_dir().or_err("Unable to resolve current directory")?;
            let temp_dir = std::env::temp_dir();
            let uid = uuid::Uuid::new_v4().simple().to_string();

            let mut data = runtime_data.lock().await;
            let deployment = data.deployment.clone().or_err("Missing deployment data")?;
            let volumes = deployment.volumes.clone();

            let manager_sock = temp_dir.join(format!("{}.sock", uid));
            let vpn_remote = data.vpn.clone();
            let inet_remote = data.inet.clone();

            let mut cmd = process::Command::new(runtime_dir.join(FILE_RUNTIME));
            cmd.current_dir(&runtime_dir);
            cmd.args([
                "-m",
                format!("{}M", deployment.mem_mib).as_str(),
                "-nographic",
                "-kernel",
                FILE_VMLINUZ,
                "-initrd",
                FILE_INITRAMFS,
                "-enable-kvm",
                "-cpu",
                "host",
                "-smp",
                deployment.cpu_cores.to_string().as_str(),
                "-append",
                "console=ttyS0 panic=1",
                "-device",
                "virtio-serial",
                "-device",
                "virtio-rng-pci",
                "-chardev",
                format!(
                    // "socket,path={},server=on,nowait,id=manager_cdev",
                    "socket,path={},server=on,wait=off,id=manager_cdev",
                    manager_sock.display()
                )
                .as_str(),
                "-device",
                "virtserialport,chardev=manager_cdev,name=manager_port",
                "-drive",
                format!(
                    "file={},cache=unsafe,readonly=on,format=raw,if=virtio",
                    deployment.task_package.display()
                )
                .as_str(),
                "-no-reboot",
            ]);

            if let Some(pci_device_id) = &data.pci_device_id {
                cmd.arg("-device");
                cmd.arg(format!("vfio-pci,host={}", pci_device_id).as_str());
            } else {
                cmd.arg("-vga");
                cmd.arg("none");
            }

            let (vpn, inet) =
            // backward-compatibility mode
            if vpn_remote.is_none() && inet_remote.is_none() {
                cmd.args(["-net", "none"]);

                let vpn = configure_chardev_endpoint(&mut cmd, "vpn", &temp_dir, &uid)?;
                let inet = configure_chardev_endpoint(&mut cmd, "inet", &temp_dir, &uid)?;
                (vpn, inet)
            // virtio-net (preferred)
            } else {
                let mut pair = SocketPairConf::default();
                pair.probe().await?;

                let vpn = configure_netdev_endpoint(&mut cmd, "vpn", &vpn_remote, pair.first)?;
                let inet = configure_netdev_endpoint(&mut cmd, "inet", &inet_remote, pair.second)?;
                (vpn, inet)
            };

            data.vpn.replace(vpn);
            data.inet.replace(inet);

            for (idx, volume) in volumes.iter().enumerate() {
                cmd.arg("-virtfs");
                cmd.arg(format!(
                    "local,id={tag},path={path},security_model=none,mount_tag={tag}",
                    tag = format!("mnt{}", idx),
                    path = work_dir.join(&volume.name).to_string_lossy(),
                ));
            }

            log::info!("Executing command: {cmd:?}");

            let mut runtime = cmd
                .stdin(Stdio::null())
                .stdout(Stdio::piped()) // why not?
                .kill_on_drop(true)
                .spawn()?;

            // let stdout = runtime.stdout.take().unwrap();
            // spawn(reader_to_log(stdout));

            let ns = notifications.clone();
            let ga = GuestAgent::connected(manager_sock, 10, move |n, _g| {
                let notifications = ns.clone();
                async move { notifications.clone().handle(n) }.boxed()
            })
            .await?;

            data.runtime.replace(runtime);
            data.ga.replace(ga);

            Ok(())
        }.await;

        let run_process: ya_runtime_sdk::RunProcess = server::RunProcess {
            bin: "/ya-self-test".into(),
            args: vec!["ya-self-test".into()],
            work_dir: "/".into(),
            ..Default::default()
        };

        log::info!("Runtime: {:?}", runtime.data);
        log::info!("Self test process: {run_process:?}");
        
        let ga = runtime.data.lock().await.ga().unwrap();
        let r_data = runtime.data.clone();

        let pid: u64 = {   
            // let no_redir = [None, None, None];
            let no_redir = &[
                None,
                Some(RedirectFdType::RedirectFdPipeBlocking(0x1000)),
                Some(RedirectFdType::RedirectFdPipeBlocking(0x1000)),
            ];
            let run = run_process;
            let data = r_data.lock().await;
            let deployment = data.deployment().expect("Runtime not started");
        
            let (uid, gid) = deployment.user;
            let env = deployment.env();
            let cwd = deployment
                .config
                .working_dir
                .as_ref()
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.as_str())
                .unwrap_or_else(|| "/");
        
            log::debug!("got run process: {:?}", run);
            log::debug!("work dir: {:?}", deployment.config.working_dir);
        
            data
                .ga()
                .unwrap()
                .lock()
                .await
                .run_process(
                    &run.bin,
                    run.args
                        .iter()
                        .map(|s| s.as_ref())
                        .collect::<Vec<&str>>()
                        .as_slice(),
                    Some(&env[..]),
                    uid,
                    gid,
                    &no_redir,
                    Some(cwd),
                )
                .await.unwrap()
                .expect("Run process failed")
        };
        
        tokio::select! {
            _ = notifications.process_died.notified() => (),
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => ()
        } 

        let out_str = async {
            let mut ga = ga.lock().await;

            let mut out = ga
                .query_output(pid, 1, 0, u64::MAX)
                .await.unwrap()
                .unwrap_or(vec![]);

            let out_str = String::from_utf8(out.clone()).unwrap_or("".into());
            println!("Stdout: {out_str}");

            let mut err = ga
                .query_output(pid, 2, 0, u64::MAX)
                .await.unwrap()
                .unwrap_or(vec![]);
            let err_str = String::from_utf8(err.clone()).unwrap_or("".into());
            println!("Stderr: {err_str}");
            out_str
        }.await;

        let status = Ok(Value::from_str(&out_str)
            .unwrap_or(Value::from_str(&format!("{{ 
                \"stdout\": \"{out_str}\" 
            }}"))
            .unwrap()));

        log::info!("Process finished");
        let result = handle_result(status).expect("Handles test result");
        if !result.is_empty() {
            println!("{result}");
        }

        log::info!("Stopping runtime");
        crate::stop(runtime.data.clone())
            .await
            .expect("Stops runtime");

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
    let stderr = String::from_utf8_lossy(&stderr).to_string();
    log::debug!("Self-test stderr: {stderr}");
    let stdout = String::from_utf8_lossy(&stdout).to_string();
    log::debug!("Self-test stdout: {stdout}");
    if return_code != 0 {
        bail!(stderr)
    }
    match serde_json::from_str(&stdout) {
        Ok(response) => Ok(response),
        Err(err) => {
            if !stderr.is_empty() {
                bail!(stderr)
            } else {
                bail!(err)
            }
        }
    }
}
