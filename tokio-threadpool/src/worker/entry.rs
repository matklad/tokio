use park::{BoxPark, BoxUnpark};
use task::{Task, Queue};
use worker::state::{State, PUSHED_MASK};

use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::mem;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::atomic::Ordering::{Acquire, AcqRel, Relaxed};
use std::time::Duration;

use crossbeam_utils::CachePadded;
use deque;
use fnv::FnvHashSet;

// How many `register_task()` and `unregister_task()` calls need to happen between task cleanups
// with `cleanup_completed_tasks()`.
const TASK_CLEANUP_INTERVAL: usize = 100;

// TODO: None of the fields should be public
//
// It would also be helpful to split up the state across what fields /
// operations are thread-safe vs. which ones require ownership of the worker.
pub(crate) struct WorkerEntry {
    // Worker state. This is mutated when notifying the worker.
    //
    // The `usize` value is deserialized to a `worker::State` instance. See
    // comments on that type.
    pub state: CachePadded<AtomicUsize>,

    // Ticker for triggering task cleanup with `cleanup_completed_tasks()`.
    ticker: Cell<usize>,

    // Set of tasks registered in this worker entry.
    owned_tasks: UnsafeCell<FnvHashSet<*const Task>>,

    // List of tasks that were stolen from this worker entry and then completed.
    completed_tasks: Mutex<Vec<Arc<Task>>>,

    // Next entry in the parked Trieber stack
    next_sleeper: UnsafeCell<usize>,

    // Worker half of deque
    worker: deque::Worker<Arc<Task>>,

    // Stealer half of deque
    stealer: deque::Stealer<Arc<Task>>,

    // Thread parker
    park: UnsafeCell<Option<BoxPark>>,

    // Thread unparker
    unpark: UnsafeCell<Option<BoxUnpark>>,

    // MPSC queue of jobs submitted to the worker from an external source.
    pub inbound: Queue,
}

impl WorkerEntry {
    pub fn new(park: BoxPark, unpark: BoxUnpark) -> Self {
        let (w, s) = deque::fifo();

        WorkerEntry {
            state: CachePadded::new(AtomicUsize::new(State::default().into())),
            ticker: Cell::new(0),
            owned_tasks: UnsafeCell::new(FnvHashSet::default()),
            completed_tasks: Mutex::new(Vec::new()),
            next_sleeper: UnsafeCell::new(0),
            worker: w,
            stealer: s,
            park: UnsafeCell::new(Some(park)),
            unpark: UnsafeCell::new(Some(unpark)),
            inbound: Queue::new(),
        }
    }

    /// Atomically load the worker's state
    ///
    /// # Ordering
    ///
    /// An `Acquire` ordering is established on the entry's state variable.
    pub fn load_state(&self) -> State {
        self.state.load(Acquire).into()
    }

    /// Atomically unset the pushed flag.
    ///
    /// # Return
    ///
    /// The state *before* the push flag is unset.
    ///
    /// # Ordering
    ///
    /// The specified ordering is established on the entry's state variable.
    pub fn fetch_unset_pushed(&self, ordering: Ordering) -> State {
        self.state.fetch_and(!PUSHED_MASK, ordering).into()
    }

    /// Submit a task to this worker while currently on the same thread that is
    /// running the worker.
    #[inline]
    pub fn submit_internal(&self, task: Arc<Task>) {
        self.push_internal(task);
    }

    /// Submits a task to the worker. This assumes that the caller is external
    /// to the worker. Internal submissions go through another path.
    ///
    /// Returns `false` if the worker needs to be spawned.
    ///
    /// # Ordering
    ///
    /// The `state` must have been obtained with an `Acquire` ordering.
    pub fn submit_external(&self, task: Arc<Task>, mut state: State) -> bool {
        use worker::Lifecycle::*;

        // Push the task onto the external queue
        self.push_external(task);

        loop {
            let mut next = state;
            next.notify();

            let actual = self.state.compare_and_swap(
                state.into(), next.into(),
                AcqRel).into();

            if state == actual {
                break;
            }

            state = actual;
        }

        match state.lifecycle() {
            Sleeping => {
                // The worker is currently sleeping, the condition variable must
                // be signaled
                self.unpark();
                true
            }
            Shutdown => false,
            Running | Notified | Signaled => {
                // In these states, the worker is active and will eventually see
                // the task that was just submitted.
                true
            }
        }
    }

    /// Signals to the worker that it should stop
    ///
    /// `state` is the last observed state for the worker. This allows skipping
    /// the initial load from the state atomic.
    ///
    /// # Return
    ///
    /// Returns `Ok` when the worker was successfully signaled.
    ///
    /// Returns `Err` if the worker has already terminated.
    pub fn signal_stop(&self, mut state: State) {
        use worker::Lifecycle::*;

        // Transition the worker state to signaled
        loop {
            let mut next = state;

            match state.lifecycle() {
                Shutdown => {
                    return;
                }
                Running | Sleeping => {}
                Notified | Signaled => {
                    // These two states imply that the worker is active, thus it
                    // will eventually see the shutdown signal, so we don't need
                    // to do anything.
                    //
                    // The worker is forced to see the shutdown signal
                    // eventually as:
                    //
                    // a) No more work will arrive
                    // b) The shutdown signal is stored as the head of the
                    // sleep, stack which will prevent the worker from going to
                    // sleep again.
                    return;
                }
            }

            next.set_lifecycle(Signaled);

            let actual = self.state.compare_and_swap(
                state.into(), next.into(), AcqRel).into();

            if actual == state {
                break;
            }

            state = actual;
        }

        // Unpark the worker
        self.unpark();
    }

    /// Pop a task
    ///
    /// This **must** only be called by the thread that owns the worker entry.
    /// This function is not `Sync`.
    pub fn pop_task(&self) -> deque::Pop<Arc<Task>> {
        self.worker.pop()
    }

    /// Steal tasks
    ///
    /// This is called by *other* workers to steal a task for processing. This
    /// function is `Sync`.
    ///
    /// At the same time, this method steals some additional tasks and moves
    /// them into `dest` in order to balance the work distribution among
    /// workers.
    pub fn steal_tasks(&self, dest: &Self) -> deque::Steal<Arc<Task>> {
        self.stealer.steal_many(&dest.worker)
    }

    /// Drain (and drop) all tasks that are queued for work.
    ///
    /// This is called when the pool is shutting down.
    pub fn drain_tasks(&self) {
        use deque::Pop;

        loop {
            match self.worker.pop() {
                Pop::Data(_) => {}
                Pop::Empty => break,
                Pop::Retry => {}
            }
        }
    }

    /// Parks the worker thread.
    pub fn park(&self) {
        if let Some(park) = unsafe { (*self.park.get()).as_mut() } {
            park.park().unwrap();
        }
    }

    /// Parks the worker thread for at most `duration`.
    pub fn park_timeout(&self, duration: Duration) {
        if let Some(park) = unsafe { (*self.park.get()).as_mut() } {
            park.park_timeout(duration).unwrap();
        }
    }

    /// Unparks the worker thread.
    #[inline]
    pub fn unpark(&self) {
        if let Some(park) = unsafe { (*self.unpark.get()).as_ref() } {
            park.unpark();
        }
    }

    /// Registers a task in this worker.
    ///
    /// This is called the first time a task is polled and assigned a home worker.
    pub fn register_task(&self, task: Arc<Task>) {
        let set = unsafe { &mut *self.owned_tasks.get() };
        let raw = &*task as *const Task;
        assert!(set.insert(raw));
        mem::forget(task);

        self.ticker.set(self.ticker.get() + 1);
        if self.ticker.get() >= TASK_CLEANUP_INTERVAL {
            self.ticker.set(0);
            self.cleanup_completed_tasks();
        }
    }

    /// Unregisters a task from this worker.
    ///
    /// This is called when the task is completed by this worker.
    pub fn unregister_task(&self, task: Arc<Task>) {
        let set = unsafe { &mut *self.owned_tasks.get() };
        let raw = &*task as *const Task;
        assert!(set.remove(&raw));
        unsafe {
            drop(Arc::from_raw(raw));
        }

        self.ticker.set(self.ticker.get() + 1);
        if self.ticker.get() >= TASK_CLEANUP_INTERVAL {
            self.ticker.set(0);
            self.cleanup_completed_tasks();
        }
    }

    /// Informs the worker that a stolen task has been completed.
    ///
    /// This is called when the task is completed by another worker.
    pub fn completed_task(&self, task: Arc<Task>) {
        self.completed_tasks.lock().unwrap().push(task);
    }

    /// Drop the remaining incomplete tasks and the parker associated with this worker.
    ///
    /// This is called by the shutdown trigger.
    pub fn shutdown(&self) {
        self.cleanup_completed_tasks();

        let set = unsafe { &mut *self.owned_tasks.get() };
        for raw in set.drain() {
            let task = unsafe { Arc::from_raw(raw) };
            task.abort();
        }

        unsafe {
            *self.park.get() = None;
            *self.unpark.get() = None;
        }
    }

    /// Unregisters all tasks in `completed_tasks`.
    fn cleanup_completed_tasks(&self) {
        let set = unsafe { &mut *self.owned_tasks.get() };
        let tasks = mem::replace(&mut *self.completed_tasks.lock().unwrap(), Vec::new());

        for task in tasks {
            let raw = &*task as *const Task;
            assert!(set.remove(&raw));
            unsafe {
                drop(Arc::from_raw(raw));
            }
        }
    }

    #[inline]
    fn push_external(&self, task: Arc<Task>) {
        self.inbound.push(task);
    }

    #[inline]
    pub fn push_internal(&self, task: Arc<Task>) {
        self.worker.push(task);
    }

    #[inline]
    pub fn next_sleeper(&self) -> usize {
        unsafe { *self.next_sleeper.get() }
    }

    #[inline]
    pub fn set_next_sleeper(&self, val: usize) {
        unsafe { *self.next_sleeper.get() = val; }
    }
}

impl fmt::Debug for WorkerEntry {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("WorkerEntry")
            .field("state", &self.state.load(Relaxed))
            .field("next_sleeper", &"UnsafeCell<usize>")
            .field("worker", &self.worker)
            .field("stealer", &self.stealer)
            .field("park", &"UnsafeCell<BoxPark>")
            .field("unpark", &"BoxUnpark")
            .field("inbound", &self.inbound)
            .finish()
    }
}
