use task::Task;
use worker::WorkerId;

use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::mem;
use std::sync::{Arc, Mutex};

use crossbeam_utils::CachePadded;

// How many `register_task()` and `unregister_task()` calls need to happen between task cleanups
// with `cleanup_completed_tasks()`.
const TASK_CLEANUP_INTERVAL: usize = 100;

struct Local {
    // Set of tasks registered in this worker entry.
    owned_tasks: Vec<*const Task>,

    // List of tasks that were stolen from this worker entry and then completed.
    completed_tasks: Mutex<Vec<Arc<Task>>>,

    // Ticker for triggering task cleanup with `cleanup_completed_tasks()`.
    ticker: Cell<usize>,
}

impl Local {
    fn new() -> Local { // TODO: rename to Shard?
        Local {
            owned_tasks: Vec::new(),
            completed_tasks: Mutex::new(Vec::new()),
            ticker: Cell::new(0),
        }
    }

    fn register(&mut self, worker_id: &WorkerId, task: &Arc<Task>) {
        task.registered_in.set((worker_id.0, self.owned_tasks.len()));
        self.owned_tasks.push(Arc::into_raw(task.clone()));

        self.ticker.set(self.ticker.get() + 1);
        if self.ticker.get() >= TASK_CLEANUP_INTERVAL {
            self.ticker.set(0);
            self.cleanup_completed_tasks(worker_id);
        }
    }

    pub fn unregister(&mut self, worker_id: &WorkerId, task: &Arc<Task>) {
        let (id, index) = task.registered_in.get();
        assert_eq!(id, worker_id.0);

        unsafe {
            drop(Arc::from_raw(self.owned_tasks.swap_remove(index)));
            if index < self.owned_tasks.len() {
                (*self.owned_tasks[index]).registered_in.set((id, index));
            }
        }

        self.ticker.set(self.ticker.get() + 1);
        if self.ticker.get() >= TASK_CLEANUP_INTERVAL {
            self.ticker.set(0);
            self.cleanup_completed_tasks(worker_id);
        }
    }

    /// Unregisters all tasks in `completed_tasks`.
    fn cleanup_completed_tasks(&mut self, worker_id: &WorkerId) {
        let completed = mem::replace(&mut *self.completed_tasks.lock().unwrap(), Vec::new());

        for task in completed {
            unsafe {
                let (id, index) = (*task).registered_in.get();
                assert_eq!(id, worker_id.0);

                drop(Arc::from_raw(self.owned_tasks.swap_remove(index)));
                if index < self.owned_tasks.len() {
                    (*self.owned_tasks[index]).registered_in.set((id, index));
                }
            }
        }
    }

    fn abort_tasks(&mut self, worker_id: &WorkerId) {
        self.cleanup_completed_tasks(&worker_id);

        for raw in self.owned_tasks.drain(..) {
            unsafe {
                let task = Arc::from_raw(raw);
                task.abort();
            }
        }
    }
}

pub struct Registry {
    locals: Vec<CachePadded<UnsafeCell<Local>>>,
}

impl Registry {
    pub fn new(num_workers: usize) -> Registry {
        let locals = (0..num_workers)
            .map(|_| {
                CachePadded::new(UnsafeCell::new(Local::new()))
            })
            .collect();

        Registry { locals }
    }

    /// Registers a task in this worker.
    ///
    /// This is called the first time a task is polled and assigned a home worker.
    pub(crate) fn before_poll(&self, worker_id: &WorkerId, task: &Arc<Task>) {
        let (id, index) = task.registered_in.get();

        if id == !0 {
            let local = self.locals[worker_id.0].get();
            let local = unsafe { &mut *local };
            local.register(worker_id, task);
        }
    }

    /// Unregisters a task from this worker.
    ///
    /// This is called when the task is completed by this worker.
    pub(crate) fn task_completed(&self, worker_id: &WorkerId, task: Arc<Task>) {
        let (id, index) = task.registered_in.get();
        assert_ne!(index, !0);

        let local = self.locals[id].get();
        let local = unsafe { &mut *local };
        if worker_id.0 == id {
            local.unregister(worker_id, &task);
        } else {
            local.completed_tasks.lock().unwrap().push(task);
        }
    }

    pub fn abort_tasks(&self) {
        for (i, local) in self.locals.iter().enumerate() {
            let local = unsafe { &mut *local.get() };
            let worker_id = WorkerId(i);
            local.abort_tasks(&worker_id);
        }
    }
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad("Registry")
    }
}
