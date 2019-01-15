#![feature(test)]
#![deny(warnings)]

extern crate futures;
extern crate mio;
extern crate test;
extern crate tokio;
extern crate tokio_current_thread;
extern crate tokio_reactor;

use futures::{future, task, Async};
use tokio::runtime::current_thread::Runtime;
use tokio_current_thread::CurrentThread;
use tokio_reactor::Registration;

const NUM_SPAWN: usize = 300;
const NUM_YIELD: usize = 300;

#[bench]
fn task_switching(b: &mut test::Bencher) {
    let mut threadpool = CurrentThread::new();

    b.iter(move || {
        for _ in 0..NUM_SPAWN {
            let mut rem = NUM_YIELD;

            threadpool.spawn(future::poll_fn(move || {
                rem -= 1;

                if rem == 0 {
                    Ok(Async::Ready(()))
                } else {
                    // Notify the current task
                    task::current().notify();

                    // Not ready
                    Ok(Async::NotReady)
                }
            }));
        }

        threadpool.run().unwrap();
    });
}

#[bench]
fn reactor_driving(b: &mut test::Bencher) {
    let mut rt = Runtime::new().unwrap();

    b.iter(|| {
        for _ in 0..NUM_SPAWN {
            let (r, s) = mio::Registration::new2();
            let registration = Registration::new();
            registration.register(&r).unwrap();

            let mut rem = NUM_YIELD;
            let mut r = Some(r);

            rt.spawn(future::poll_fn(move || {
                loop {
                    let is_ready = registration.poll_read_ready().unwrap().is_ready();

                    if is_ready {
                        rem -= 1;

                        if rem == 0 {
                            r.take().unwrap();
                            return Ok(Async::Ready(()));
                        }
                    } else {
                        s.set_readiness(mio::Ready::readable()).unwrap();
                        return Ok(Async::NotReady);
                    }
                }
            }));
        }

        rt.run().unwrap();
    })
}
