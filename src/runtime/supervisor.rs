use std::{fmt, future::Future, time::Duration};

use tokio::{
    sync::watch,
    task::{AbortHandle, JoinHandle, JoinSet},
};

#[derive(Clone)]
pub struct Shutdown {
    sender: watch::Sender<bool>,
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Shutdown {
    pub fn new() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }

    pub fn start(&self) {
        self.sender.send_replace(true);
    }

    pub fn is_started(&self) -> bool {
        *self.sender.borrow()
    }

    pub async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        if *receiver.borrow() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow() {
                return;
            }
        }
    }
}

pub struct NamedTask {
    pub name: &'static str,
    pub handle: JoinHandle<()>,
}

impl NamedTask {
    pub fn new(name: &'static str, handle: JoinHandle<()>) -> Self {
        Self { name, handle }
    }
}

#[derive(Debug)]
pub struct TaskFailure {
    name: &'static str,
    kind: TaskFailureKind,
}

#[derive(Debug)]
enum TaskFailureKind {
    Exited,
    Join(tokio::task::JoinError),
    Supervisor(tokio::task::JoinError),
    Empty,
}

impl TaskFailure {
    pub fn name(&self) -> &'static str {
        self.name
    }
}

impl fmt::Display for TaskFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            TaskFailureKind::Exited => {
                write!(formatter, "critical task {} exited unexpectedly", self.name)
            }
            TaskFailureKind::Join(error) => {
                write!(formatter, "critical task {} failed: {error}", self.name)
            }
            TaskFailureKind::Supervisor(error) => {
                write!(formatter, "task supervisor failed: {error}")
            }
            TaskFailureKind::Empty => formatter.write_str("task supervisor has no tasks"),
        }
    }
}

impl std::error::Error for TaskFailure {}

pub struct TaskSupervisor {
    tasks: JoinSet<(&'static str, Result<(), tokio::task::JoinError>)>,
    abort_handles: Vec<AbortHandle>,
}

impl Default for TaskSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskSupervisor {
    pub fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            abort_handles: Vec::new(),
        }
    }

    pub fn supervise(&mut self, task: NamedTask) {
        let NamedTask { name, handle } = task;
        self.abort_handles.push(handle.abort_handle());
        self.tasks.spawn(async move { (name, handle.await) });
    }

    pub fn spawn<F>(&mut self, name: &'static str, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.supervise(NamedTask::new(name, tokio::spawn(future)));
    }

    pub async fn next_failure(&mut self) -> TaskFailure {
        match self.tasks.join_next().await {
            Some(Ok((name, Ok(())))) => TaskFailure {
                name,
                kind: TaskFailureKind::Exited,
            },
            Some(Ok((name, Err(error)))) => TaskFailure {
                name,
                kind: TaskFailureKind::Join(error),
            },
            Some(Err(error)) => TaskFailure {
                name: "supervisor",
                kind: TaskFailureKind::Supervisor(error),
            },
            None => TaskFailure {
                name: "supervisor",
                kind: TaskFailureKind::Empty,
            },
        }
    }

    pub async fn drain(&mut self, grace: Duration) -> bool {
        let drained = tokio::time::timeout(grace, async {
            let mut clean = true;
            while let Some(result) = self.tasks.join_next().await {
                match result {
                    Ok((name, Ok(()))) => {
                        tracing::debug!(target: "runtime", task = name, "task stopped");
                    }
                    Ok((name, Err(error))) => {
                        clean = false;
                        tracing::error!(target: "runtime", task = name, %error, "task failed during shutdown");
                    }
                    Err(error) => {
                        clean = false;
                        tracing::error!(target: "runtime", %error, "task supervisor failed during shutdown");
                    }
                }
            }
            clean
        })
        .await;

        let Ok(clean) = drained else {
            for handle in &self.abort_handles {
                handle.abort();
            }
            self.tasks.abort_all();
            while self.tasks.join_next().await.is_some() {}
            return false;
        };
        if !clean {
            return false;
        }
        true
    }
}

impl Drop for TaskSupervisor {
    fn drop(&mut self) {
        for handle in &self.abort_handles {
            handle.abort();
        }
        self.tasks.abort_all();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn reports_the_name_of_an_unexpected_exit() {
        let mut supervisor = TaskSupervisor::new();
        supervisor.spawn("engine", async {});

        let failure = supervisor.next_failure().await;
        assert_eq!(failure.name(), "engine");
        assert_eq!(
            failure.to_string(),
            "critical task engine exited unexpectedly"
        );
    }

    #[tokio::test]
    async fn reports_the_name_of_a_panicked_task() {
        let mut supervisor = TaskSupervisor::new();
        supervisor.spawn("pricing", async { panic!("injected task panic") });

        let failure = supervisor.next_failure().await;
        assert_eq!(failure.name(), "pricing");
        assert!(failure.to_string().contains("critical task pricing failed"));
    }

    #[tokio::test]
    async fn cancellation_drains_cooperative_tasks() {
        let shutdown = Shutdown::new();
        let listener = shutdown.clone();
        let mut supervisor = TaskSupervisor::new();
        supervisor.spawn("cooperative", async move { listener.cancelled().await });

        shutdown.start();
        assert!(supervisor.drain(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn shutdown_grace_aborts_a_stuck_task() {
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let mut supervisor = TaskSupervisor::new();
        supervisor.spawn("stuck", async move {
            let _drop_flag = DropFlag(task_dropped);
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;

        assert!(!supervisor.drain(Duration::from_millis(10)).await);
        assert!(dropped.load(Ordering::Acquire));
    }
}
