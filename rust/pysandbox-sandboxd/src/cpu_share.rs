use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicU64, Ordering},
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::time::{Sleep, sleep_until};

const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_ACTIVITY_TIMEOUT: Duration = Duration::from_millis(300);
const FUEL_RATE_SMOOTHING: f64 = 0.25;

#[derive(Clone, Copy, Debug)]
pub struct CpuShareConfig {
    pub enabled: bool,
    pub limit_percent: Option<f64>,
    pub sample_interval: Duration,
    pub activity_timeout: Duration,
    pub fuel_yield_interval: u64,
}

impl CpuShareConfig {
    pub fn new(fuel_yield_interval: u64) -> Self {
        Self {
            enabled: false,
            limit_percent: None,
            sample_interval: DEFAULT_SAMPLE_INTERVAL,
            activity_timeout: DEFAULT_ACTIVITY_TIMEOUT,
            fuel_yield_interval,
        }
    }
}

#[derive(Debug)]
struct WorkerState {
    last_active: Instant,
    window_fuel: u64,
    weight: u64,
}

#[derive(Debug)]
struct CpuShareState {
    workers: HashMap<u64, WorkerState>,
    window_started: Instant,
    window_fuel: u64,
    estimated_fuel_per_core_second: Option<f64>,
    measured_cores: f64,
}

impl Default for CpuShareState {
    fn default() -> Self {
        Self {
            workers: HashMap::new(),
            window_started: Instant::now(),
            window_fuel: 0,
            estimated_fuel_per_core_second: None,
            measured_cores: 0.0,
        }
    }
}

#[derive(Debug)]
pub struct CpuShare {
    config: CpuShareConfig,
    state: Mutex<CpuShareState>,
}

impl CpuShare {
    pub fn start(mut config: CpuShareConfig) -> Arc<Self> {
        config.sample_interval = config
            .sample_interval
            .max(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        let scheduler = Arc::new(Self {
            config,
            state: Mutex::new(CpuShareState::default()),
        });
        if config.enabled {
            tokio::spawn(sample_cpu(
                Arc::downgrade(&scheduler),
                config.sample_interval,
            ));
        }
        scheduler
    }

    pub fn worker(self: &Arc<Self>, worker_id: u64) -> CpuShareWorker {
        CpuShareWorker {
            scheduler: self.clone(),
            worker_id,
            weight: AtomicU64::new(1),
            last_fuel: Mutex::new(None),
            throttle_until: Mutex::new(None),
        }
    }

    fn record_fuel(
        &self,
        worker_id: u64,
        consumed: u64,
        weight: u64,
        now: Instant,
    ) -> Option<Instant> {
        if !self.config.enabled || consumed == 0 {
            return None;
        }

        let mut state = self.state.lock().expect("CPU share lock poisoned");
        state.workers.retain(|_, worker| {
            now.saturating_duration_since(worker.last_active) <= self.config.activity_timeout
        });

        let worker = state.workers.entry(worker_id).or_insert(WorkerState {
            last_active: now,
            window_fuel: 0,
            weight,
        });
        worker.last_active = now;
        worker.weight = weight;
        worker.window_fuel = worker.window_fuel.saturating_add(consumed);
        state.window_fuel = state.window_fuel.saturating_add(consumed);

        let Some(fuel_per_core_second) = state.estimated_fuel_per_core_second else {
            return None;
        };
        let available_cores = self
            .config
            .limit_percent
            .map_or(state.measured_cores, |percent| percent / 100.0);
        let fuel_rate = fuel_per_core_second * available_cores;
        let capacity = fuel_rate * self.config.sample_interval.as_secs_f64();
        let allowance = weighted_fair_allowance(worker_id, &state.workers, capacity);
        let worker_fuel = state
            .workers
            .get(&worker_id)
            .expect("active CPU share worker disappeared")
            .window_fuel as f64;
        if worker_fuel <= allowance + self.config.fuel_yield_interval as f64 {
            return None;
        }

        Some(state.window_started + self.config.sample_interval)
    }

    fn finish_worker(&self, worker_id: u64) {
        self.state
            .lock()
            .expect("CPU share lock poisoned")
            .workers
            .remove(&worker_id);
    }

    fn set_worker_weight(&self, worker_id: u64, weight: u64) {
        if let Some(worker) = self
            .state
            .lock()
            .expect("CPU share lock poisoned")
            .workers
            .get_mut(&worker_id)
        {
            worker.weight = weight;
        }
    }

    fn sample(&self, measured_cores: f64, now: Instant) {
        let mut state = self.state.lock().expect("CPU share lock poisoned");
        let elapsed = now.saturating_duration_since(state.window_started);
        if !elapsed.is_zero() && state.window_fuel != 0 && measured_cores > 0.0 {
            let fuel_per_core_second =
                state.window_fuel as f64 / (measured_cores * elapsed.as_secs_f64());
            state.estimated_fuel_per_core_second =
                Some(match state.estimated_fuel_per_core_second {
                    Some(previous) => {
                        previous * (1.0 - FUEL_RATE_SMOOTHING)
                            + fuel_per_core_second * FUEL_RATE_SMOOTHING
                    }
                    None => fuel_per_core_second,
                });
        }

        state.measured_cores = measured_cores;
        state.window_started = now;
        state.window_fuel = 0;
        state.workers.retain(|_, worker| {
            now.saturating_duration_since(worker.last_active) <= self.config.activity_timeout
        });
        for worker in state.workers.values_mut() {
            worker.window_fuel = 0;
        }
    }
}

fn weighted_fair_allowance(
    worker_id: u64,
    workers: &HashMap<u64, WorkerState>,
    capacity: f64,
) -> f64 {
    let mut unsatisfied = workers
        .iter()
        .map(|(&id, worker)| (id, worker.window_fuel as f64, worker.weight as f64))
        .collect::<Vec<_>>();
    let mut remaining_capacity = capacity;
    let mut remaining_weight = unsatisfied.iter().map(|(_, _, weight)| weight).sum::<f64>();

    loop {
        let Some(index) = unsatisfied.iter().position(|(_, demand, weight)| {
            *demand <= remaining_capacity * *weight / remaining_weight
        }) else {
            return unsatisfied
                .iter()
                .find(|(id, _, _)| *id == worker_id)
                .map_or(0.0, |(_, _, weight)| {
                    remaining_capacity * *weight / remaining_weight
                });
        };
        let (id, demand, weight) = unsatisfied.swap_remove(index);
        if id == worker_id {
            return demand;
        }
        remaining_capacity = (remaining_capacity - demand).max(0.0);
        remaining_weight -= weight;
        if unsatisfied.is_empty() || remaining_weight <= 0.0 {
            return 0.0;
        }
    }
}

#[derive(Debug)]
pub struct CpuShareWorker {
    scheduler: Arc<CpuShare>,
    worker_id: u64,
    weight: AtomicU64,
    last_fuel: Mutex<Option<u64>>,
    throttle_until: Mutex<Option<Instant>>,
}

impl CpuShareWorker {
    pub fn begin(&self, fuel: u64, weight: u64) {
        self.set_weight(weight);
        *self.last_fuel.lock().expect("CPU share fuel lock poisoned") = Some(fuel);
        *self
            .throttle_until
            .lock()
            .expect("CPU share throttle lock poisoned") = None;
    }

    pub fn set_weight(&self, weight: u64) {
        self.weight.store(weight, Ordering::Release);
        self.scheduler.set_worker_weight(self.worker_id, weight);
    }

    pub fn observe_fuel(&self, remaining: u64) {
        let consumed = {
            let mut previous = self.last_fuel.lock().expect("CPU share fuel lock poisoned");
            let consumed = previous.map_or(0, |value| value.saturating_sub(remaining));
            *previous = Some(remaining);
            consumed
        };
        if let Some(deadline) = self.scheduler.record_fuel(
            self.worker_id,
            consumed,
            self.weight.load(Ordering::Acquire),
            Instant::now(),
        ) {
            let mut throttle = self
                .throttle_until
                .lock()
                .expect("CPU share throttle lock poisoned");
            *throttle = Some(throttle.map_or(deadline, |current| current.max(deadline)));
        }
    }

    pub fn reset_fuel(&self, remaining: u64) {
        *self.last_fuel.lock().expect("CPU share fuel lock poisoned") = Some(remaining);
    }

    pub fn finish(&self) {
        self.scheduler.finish_worker(self.worker_id);
        *self.last_fuel.lock().expect("CPU share fuel lock poisoned") = None;
    }

    pub async fn run<F: Future>(&self, future: F) -> F::Output {
        CpuSharedFuture {
            inner: Box::pin(future),
            worker: self,
            sleep: None,
        }
        .await
    }

    fn throttle_deadline(&self) -> Option<Instant> {
        let mut deadline = self
            .throttle_until
            .lock()
            .expect("CPU share throttle lock poisoned");
        if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
            *deadline = None;
        }
        *deadline
    }
}

struct CpuSharedFuture<'a, F> {
    inner: Pin<Box<F>>,
    worker: &'a CpuShareWorker,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl<F: Future> Future for CpuSharedFuture<'_, F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(deadline) = self.worker.throttle_deadline() {
            let replace_sleep = self
                .sleep
                .as_ref()
                .is_none_or(|sleep| sleep.deadline() != deadline.into());
            if replace_sleep {
                self.sleep = Some(Box::pin(sleep_until(deadline.into())));
            }
            if self
                .sleep
                .as_mut()
                .expect("CPU share sleep was just installed")
                .as_mut()
                .poll(context)
                .is_pending()
            {
                return Poll::Pending;
            }
            self.sleep = None;
        }

        self.inner.as_mut().poll(context)
    }
}

async fn sample_cpu(scheduler: Weak<CpuShare>, sample_interval: Duration) {
    let Ok(pid) = sysinfo::get_current_pid() else {
        return;
    };
    let mut system = System::new();
    let refresh = ProcessRefreshKind::nothing().with_cpu();
    refresh_process(&mut system, pid, refresh);

    let mut interval = tokio::time::interval(sample_interval);
    interval.tick().await;
    loop {
        interval.tick().await;
        let Some(scheduler) = scheduler.upgrade() else {
            return;
        };
        refresh_process(&mut system, pid, refresh);
        if let Some(process) = system.process(pid) {
            scheduler.sample(process.cpu_usage() as f64 / 100.0, Instant::now());
        }
    }
}

fn refresh_process(system: &mut System, pid: Pid, refresh: ProcessRefreshKind) {
    system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, refresh);
}

#[cfg(test)]
mod tests {
    use super::{CpuShare, CpuShareConfig, WorkerState, weighted_fair_allowance};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn active_workers_split_the_estimated_fuel_rate() {
        let config = CpuShareConfig {
            enabled: true,
            limit_percent: Some(100.0),
            sample_interval: Duration::from_millis(100),
            activity_timeout: Duration::from_secs(1),
            fuel_yield_interval: 10,
        };
        let scheduler = CpuShare {
            config,
            state: Default::default(),
        };
        let now = Instant::now();
        scheduler.sample(1.0, now);
        {
            let mut state = scheduler.state.lock().unwrap();
            state.estimated_fuel_per_core_second = Some(1_000.0);
            state.measured_cores = 1.0;
            state.window_started = now;
        }

        assert_eq!(scheduler.record_fuel(1, 50, 1, now), None);
        assert_eq!(scheduler.record_fuel(2, 50, 1, now), None);
        assert_eq!(
            scheduler.record_fuel(1, 20, 1, now),
            Some(now + config.sample_interval)
        );
    }

    #[test]
    fn inactive_workers_stop_reducing_the_share() {
        let config = CpuShareConfig {
            enabled: true,
            limit_percent: Some(100.0),
            sample_interval: Duration::from_millis(100),
            activity_timeout: Duration::from_millis(10),
            fuel_yield_interval: 0,
        };
        let scheduler = CpuShare {
            config,
            state: Default::default(),
        };
        let now = Instant::now();
        {
            let mut state = scheduler.state.lock().unwrap();
            state.estimated_fuel_per_core_second = Some(1_000.0);
            state.measured_cores = 1.0;
            state.window_started = now;
        }
        scheduler.record_fuel(1, 10, 1, now);
        scheduler.record_fuel(2, 10, 1, now);

        assert_eq!(
            scheduler.record_fuel(1, 80, 1, now + Duration::from_millis(11)),
            None
        );
    }

    #[test]
    fn total_cpu_limit_caps_the_available_fuel_budget() {
        let config = CpuShareConfig {
            enabled: true,
            limit_percent: Some(50.0),
            sample_interval: Duration::from_millis(100),
            activity_timeout: Duration::from_secs(1),
            fuel_yield_interval: 0,
        };
        let scheduler = CpuShare {
            config,
            state: Default::default(),
        };
        let now = Instant::now();
        {
            let mut state = scheduler.state.lock().unwrap();
            state.estimated_fuel_per_core_second = Some(1_000.0);
            state.measured_cores = 4.0;
            state.window_started = now;
        }

        assert_eq!(
            scheduler.record_fuel(1, 60, 1, now),
            Some(now + config.sample_interval)
        );
    }

    #[test]
    fn unused_weight_is_redistributed_to_busy_workers() {
        let workers = HashMap::from([
            (
                1,
                WorkerState {
                    last_active: Instant::now(),
                    window_fuel: 10,
                    weight: 10,
                },
            ),
            (
                2,
                WorkerState {
                    last_active: Instant::now(),
                    window_fuel: 100,
                    weight: 1,
                },
            ),
        ]);

        assert_eq!(weighted_fair_allowance(1, &workers, 100.0), 10.0);
        assert_eq!(weighted_fair_allowance(2, &workers, 100.0), 90.0);
    }

    #[test]
    fn resetting_fuel_does_not_count_as_guest_consumption() {
        let config = CpuShareConfig {
            enabled: true,
            limit_percent: Some(100.0),
            sample_interval: Duration::from_millis(100),
            activity_timeout: Duration::from_secs(1),
            fuel_yield_interval: 10,
        };
        let scheduler = Arc::new(CpuShare {
            config,
            state: Default::default(),
        });
        let worker = scheduler.worker(1);
        worker.begin(1_000, 1);
        worker.reset_fuel(10);
        worker.observe_fuel(9);

        let state = scheduler.state.lock().unwrap();
        assert_eq!(state.window_fuel, 1);
    }

    #[tokio::test]
    async fn worker_waits_after_exceeding_its_allowance() {
        let config = CpuShareConfig {
            enabled: true,
            limit_percent: Some(100.0),
            sample_interval: Duration::from_millis(40),
            activity_timeout: Duration::from_secs(1),
            fuel_yield_interval: 0,
        };
        let scheduler = Arc::new(CpuShare {
            config,
            state: Default::default(),
        });
        let window_started = Instant::now();
        {
            let mut state = scheduler.state.lock().unwrap();
            state.estimated_fuel_per_core_second = Some(1_000.0);
            state.measured_cores = 1.0;
            state.window_started = window_started;
        }
        let worker = scheduler.worker(1);
        worker.begin(1_000, 1);
        worker.observe_fuel(900);

        let started = Instant::now();
        assert_eq!(worker.run(async { 42 }).await, 42);
        assert!(started.elapsed() >= Duration::from_millis(20));
    }
}
