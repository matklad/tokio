use task::Task;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(crate) struct Registry {
    // Set of tasks registered in this worker entry.
    tasks: Vec<*const Task>,
}

impl Registry {
    pub fn new() -> Registry {
        Registry {
            tasks: Vec::new(),
        }
    }

    #[inline]
    pub fn add(&mut self, task: &Arc<Task>) {
        task.reg_index.store(self.tasks.len(), Ordering::Release);
        self.tasks.push(Arc::into_raw(task.clone()));
    }

    #[inline]
    pub fn remove(&mut self, task: &Arc<Task>) {
        let index = task.reg_index.load(Ordering::Acquire);
        assert_ne!(index, !0);

        unsafe {
            drop(Arc::from_raw(self.tasks.swap_remove(index)));
            if index < self.tasks.len() {
                (*self.tasks[index]).reg_index.store(index, Ordering::Release);
            }
        }
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        for raw in self.tasks.drain(..) {
            unsafe {
                let task = Arc::from_raw(raw);
                task.abort();
            }
        }
    }
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad("Registry")
    }
}
