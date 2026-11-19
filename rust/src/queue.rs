use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::event::Event;

/// Hard cap. When the queue is full the oldest event is dropped, not the newest.
pub const CAPACITY: usize = 5000;
/// Wake the sender as soon as this many events are queued.
pub const BATCH_THRESHOLD: usize = 100;
/// The server accepts at most 200 events per request.
pub const MAX_BATCH: usize = 200;

struct State {
    items: VecDeque<Event>,
    dropped: u64,
    closed: bool,
}

/// Bounded, non-blocking queue. `push` is O(1) and never waits for a lock held
/// by the sender longer than a batch drain.
pub struct BoundedQueue {
    state: Mutex<State>,
    wake: Condvar,
    capacity: usize,
}

impl BoundedQueue {
    pub fn new(capacity: usize) -> BoundedQueue {
        BoundedQueue {
            state: Mutex::new(State {
                items: VecDeque::with_capacity(capacity.min(1024)),
                dropped: 0,
                closed: false,
            }),
            wake: Condvar::new(),
            capacity: capacity.max(1),
        }
    }

    /// Enqueue one event. Drops the oldest event when full. Never blocks on I/O.
    pub fn push(&self, event: Event) {
        let mut state = self.lock();
        if state.closed {
            return;
        }
        while state.items.len() >= self.capacity {
            state.items.pop_front();
            state.dropped += 1;
        }
        state.items.push_back(event);
        if state.items.len() >= BATCH_THRESHOLD {
            self.wake.notify_one();
        }
    }

    /// Block up to `timeout` waiting for a batch.
    ///
    /// Returns events when the threshold is reached, when `timeout` expires with
    /// a non-empty queue, or when the queue is closed and drained.
    /// Returns `None` once the queue is closed and empty.
    pub fn wait_batch(
        &self,
        timeout: Duration,
        threshold: usize,
        max: usize,
    ) -> Option<Vec<Event>> {
        let deadline = Instant::now() + timeout;
        let mut state = self.lock();
        loop {
            if state.closed {
                if state.items.is_empty() {
                    return None;
                }
                return Some(drain(&mut state, max));
            }
            if !state.items.is_empty() && state.items.len() >= threshold {
                return Some(drain(&mut state, max));
            }
            if Instant::now() >= deadline {
                // Empty or not, the flush window is over: hand back what is there.
                return Some(drain(&mut state, max));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let (guard, _) = self
                .wake
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = guard;
        }
    }

    /// Close the queue: no new events are accepted, the worker drains and exits.
    pub fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        self.wake.notify_all();
    }

    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    pub fn len(&self) -> usize {
        self.lock().items.len()
    }

    pub fn dropped(&self) -> u64 {
        self.lock().dropped
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn drain(state: &mut State, max: usize) -> Vec<Event> {
    let count = state.items.len().min(max.max(1));
    state.items.drain(..count).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Level;
    use serde_json::Value;

    fn event(code: &str) -> Event {
        Event::build(Level::Info, code, "m", Value::Null, None, "p", "s")
    }

    #[test]
    fn capacity_is_5000_by_contract() {
        assert_eq!(CAPACITY, 5000);
        assert_eq!(BATCH_THRESHOLD, 100);
        assert_eq!(MAX_BATCH, 200);
    }

    #[test]
    fn full_queue_drops_the_oldest_and_counts_it() {
        let queue = BoundedQueue::new(3);
        for code in ["a", "b", "c"] {
            queue.push(event(code));
        }
        assert_eq!(queue.len(), 3);
        queue.push(event("d"));
        assert_eq!(queue.len(), 3);
        assert_eq!(queue.dropped(), 1);

        let batch = queue
            .wait_batch(Duration::from_millis(1), BATCH_THRESHOLD, MAX_BATCH)
            .expect("batch");
        let codes: Vec<&str> = batch.iter().map(|e| e.event.as_str()).collect();
        assert_eq!(codes, vec!["b", "c", "d"]);
    }

    #[test]
    fn dropping_many_keeps_the_newest_5000() {
        let queue = BoundedQueue::new(CAPACITY);
        for i in 0..CAPACITY + 10 {
            queue.push(event(&format!("e{i}")));
        }
        assert_eq!(queue.len(), CAPACITY);
        assert_eq!(queue.dropped(), 10);
        let batch = queue
            .wait_batch(Duration::from_millis(1), CAPACITY, CAPACITY)
            .expect("batch");
        assert_eq!(batch[0].event, "e10");
        assert_eq!(batch[CAPACITY - 1].event, format!("e{}", CAPACITY + 9));
    }

    #[test]
    fn close_drains_then_returns_none() {
        let queue = BoundedQueue::new(10);
        queue.push(event("a"));
        queue.close();
        queue.push(event("b"));
        let batch = queue
            .wait_batch(Duration::from_secs(1), BATCH_THRESHOLD, MAX_BATCH)
            .expect("batch");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event, "a");
        assert!(queue
            .wait_batch(Duration::from_millis(1), BATCH_THRESHOLD, MAX_BATCH)
            .is_none());
    }
}
