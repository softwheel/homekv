use std::collections::{HashMap, HashSet};
use std::time::Duration;
use std::time::Instant;

use super::node::HoneyBee;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// A phi accrual failure detector implementation.
pub struct FailureDetector {
    /// Heartbeat samples for each node.
    node_samples: HashMap<HoneyBee, SamplingWindow>,
    /// Failure detector configuration.
    config: FailureDetectorConfig,
    /// Denotes live nodes.
    live_nodes: HashSet<HoneyBee>,
    /// Denotes dead nodes.
    dead_nodes: HashMap<HoneyBee, Instant>,
}

impl FailureDetector {
    pub fn new(config: FailureDetectorConfig) -> Self {
        Self {
            node_samples: HashMap::new(),
            config,
            live_nodes: HashSet::new(),
            dead_nodes: HashMap::new(),
        }
    }

    /// Reports node heartbeat.
    pub fn report_heartbeat(&mut self, node: &HoneyBee) {
        debug!(node = ?node, "reporting node heartbeat.");
        let heartbeat_window = self
            .node_samples
            .entry(node.clone().into())
            .or_insert_with(|| {
                SamplingWindow::new(
                    self.config.sampling_window_size,
                    self.config.max_interval,
                    self.config.initial_interval,
                )
            });
        heartbeat_window.report_heartbeat();
    }

    /// Marks a node as dead or live.
    pub fn update_node_liveliness(&mut self, node: &HoneyBee) {
        if let Some(phi) = self.phi(node) {
            debug!(node = ?node, phi = phi, "updating node liveliness");
            if phi > self.config.phi_threshold {
                node.is_alive = false;
                self.live_nodes.remove(node);
                self.dead_nodes.insert(node.clone().into(), Instant::now());
                // Remove current sampling window so that when the node
                // comes back online, we start with a fresh sampling window.
                self.node_samples.remove(node);
            } else {
                node.is_alive = true;
                self.live_nodes.insert(node.clone().into());
                self.dead_nodes.remove(node);
            }
        }
    }

    /// Removes and returns the list of garbage collectible nodes.
    pub fn garbage_collect(&mut self) -> Vec<HoneyBee> {
        let mut garbage_collected_nodes: Vec<HoneyBee> = Vec::new();
        for (node, instant) in self.dead_nodes.iter() {
            if instant.elapsed() >= self.config.dead_node_grace_period {
                garbage_collected_nodes.push(node.clone().into())
            }
        }

        for node in garbage_collected_nodes.iter() {
            self.dead_nodes.remove(node);
        }

        garbage_collected_nodes
    }

    pub fn live_nodes(&self) -> impl Iterator<Item = &HoneyBee> {
        self.live_nodes.iter()
    }

    pub fn dead_nodes(&self) -> impl Iterator<Item = &HoneyBee> {
        self.dead_nodes.iter().map(|(node, _)| node)
    }

    fn phi(&mut self, node: &HoneyBee) -> Option<f64> {
        self.node_samples
            .get(node)
            .map(|sampling_window| sampling_window.phi())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct FailureDetectorConfig {
    /// Phi threshold value above which a node is flagged as faulty.
    pub phi_threshold: f64,
    pub sampling_window_size: usize,
    /// Heartbeat longer than this will be dropped.
    pub max_interval: Duration,
    /// Initial interval used on startup when no previous heartbeat exists.
    pub initial_interval: Duration,
    /// Threshold period after which dead node can be removed from the cluster.
    pub dead_node_grace_period: Duration,
}

impl FailureDetectorConfig {
    pub fn new(
        phi_threshold: f64,
        sampling_window_size: usize,
        max_interval: Duration,
        initial_interval: Duration,
        dead_node_grace_period: Duration,
    ) -> Self {
        Self {
            phi_threshold,
            sampling_window_size,
            max_interval,
            initial_interval,
            dead_node_grace_period,
        }
    }
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            phi_threshold: 8.0,
            sampling_window_size: 1000,
            max_interval: Duration::from_secs(10),
            initial_interval: Duration::from_secs(5),
            dead_node_grace_period: Duration::from_secs(24 * 60 * 60), // 24 hours
        }
    }
}

/// A fixed-sized window that keeps track of the most recent heartbeat arrival intervals.
#[derive(Debug)]
struct SamplingWindow {
    /// The set of collected intervals.
    intervals: BoundedArrayStates,
    /// Last heartbeat reported time.
    last_heartbeat: Option<Instant>,
    /// Heartbeat intervals greater than this value are ignored.
    max_interval: Duration,
    /// The initial interval on startup.
    initial_interval: Duration,
}

impl SamplingWindow {
    pub fn new(window_size: usize, max_interval: Duration, initial_interval: Duration) -> Self {
        Self {
            intervals: BoundedArrayStats::new(window_size),
            last_heartbeat: None,
            max_interval,
            initial_interval,
        }
    }

    pub fn report_heartbeat(&mut self) {
        if let Some(last_value) = &self.last_heartbeat {
            let interval = last_value.elapsed();
            if interval <= self.max_interval {
                self.intervals.append(interval.as_secs_f64());
            } else {
                self.intervals.append(self.initial_interval.as_secs_f64());
            };
            self.last_heartbeat = Some(Instant::now());
        }
    }

    /// Computes the sampling window's phi value.
    pub fn phi(&self) -> f64 {
        // Ensure we don't all before any sample arrival.
        assert!(self.intervals.mean() > 0.0 && self.last_heartbeat.is_some());
        let elapsed_time = self.last_heartbeat.unwrap().elapsed().as_secs_f64();
        elapsed_time / self.intervals.mean()
    }
}

/// An array that retains a fixed number of streaming values.
#[derive(Debug)]
struct BoundedArrayStats {
    /// The values
    data: Vec<f64>,
    /// Number of accumulated values.
    size: usize,
    /// Is the values array filled?
    is_filled: bool,
    /// Position of the index within the values array.
    index: usize,
    sum: f64,
    /// The accumulated mean of values.
    mean: f64,
}

impl BoundedArrayStats {
    pub fn new(size: usize) -> Self {
        Self {
            data: vec![0.0; size],
            size,
            is_filled: false,
            index: 0,
            sum: 0.0,
            mean: 0.0,
        }
    }

    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Appends a new value and updates the statistics.
    pub fn append(&mut self, interval: f64) {
        if self.index == self.size {
            self.is_filled = true;
            self.index = 0;
        }

        if self.is_filled {
            self.sum -= self.data[self.index];
        }
        self.sum += interval;

        self.data[self.index] = interval;
        self.index += 1;

        self.mean = self.sum / self.len() as f64;
    }

    fn len(&self) -> usize {
        if self.is_filled {
            return self.size;
        }
        self.index
    }
}
