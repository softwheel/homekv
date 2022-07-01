//! Async Mvcc - A MVCC Transaction with Arc

use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

/// An async MVCC storage
///
/// This structure acts as `RWLock<T>`, but writes and multiple reads
/// can be performed simultaneously, which means they are not block
/// each other.
///
/// To make it possible, a pattern called `copy-on-write` is adopted.
/// As a write transaction begins, we make a new copy of the data by
/// `clone`. And writes are serialized and guaranteed exclusive.
///
/// While the data will be consistent for the duration of any read
/// transactions with different versions of the data.

#[derive(Debug, Default)]
pub struct Mvcc<T> {
    write: Mutex<()>,
    active: Mutex<Arc<T>>,
}

/// A MVCC Write Transaction.
///
/// This allows mutation of the storage without blocking readers, and
/// provides `Atomicity` for the users. Because changes are only
/// stored in this structure until you call commit. If it's failed, the data
/// won't be updated to the inner content.
pub struct MvccWriteTxn<'a, T: 'a> {
    new_data: Option<T>,
    read: Arc<T>,
    caller: &'a Mvcc<T>,
    _guard: MutexGuard<'a, ()>,
}

/// A MVCC Read Transaction
///
/// This allows safe readings without blocking writers.
#[derive(Debug)]
pub struct MvccReadTxn<T> {
    pub inner: Arc<T>,
}

impl<T> Clone for MvccReadTxn<T> {
    fn clone(&self) -> Self {
        MvccReadTxn {
            inner: self.inner.clone()
        }
    }
}

impl<T> Mvcc<T>
where
    T: Clone,
{
    pub fn new(data: T) -> Self {
        Mvcc {
            write: Mutex::new(()),
            active: Mutex::new(Arc::new(data)),
        }
    }

    pub async fn read(&self) -> MvccReadTxn<T> {
        let rwguard: MutexGuard<Arc<T>> = self.active.lock().await;
        MvccReadTxn {
            inner: rwguard.clone()
        }
    }

    pub async fn write<'x>(&'x self) -> MvccWriteTxn<'x, T> {
        let mguard: MutexGuard<()> = self.write.lock().await;
        // We delay copying until the first get_mut
        let read: Arc<T> = {
            let rwguard = self.active.lock().await;
            rwguard.clone()
        };
        MvccWriteTxn {
            new_data: None,
            read,
            caller: self,
            _guard: mguard,
        }
    }

    async fn commit(&self, new_data: Option<T>) {
        if let Some(nd) = new_data {
            let mut rwguard = self.active.lock().await;
            let new_inner = Arc::new(nd);
            // Overwrite
            *rwguard = new_inner;
        }
    }
}

impl<T> Deref for MvccReadTxn<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<'a, T> MvccWriteTxn<'a, T>
where
    T: Clone,
{
    #[inline(always)]
    pub fn get_mut(&mut self) -> &mut T {
        if self.new_data.is_none() {
            let mut data: Option<T> = Some((*self.read).clone());
            std::mem::swap(&mut data, &mut self.new_data);
            debug_assert!(data.is_none());
        }
        self.new_data.as_mut().expect("Can not change new_data as a mutable one")
    }

    pub async fn commit(self) {
        self.caller.commit(self.new_data).await;
    }
}

impl<'a, T> Deref for MvccWriteTxn<'a, T>
where
    T: Clone
{
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &T {
        match &self.new_data {
            Some(nd) => nd,
            None => &(*self.read)
        }
    }
}

impl<'a, T> DerefMut for MvccWriteTxn<'a, T>
where
    T: Clone,
{
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut T {
        self.get_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::Mvcc;

    #[tokio::test]
    async fn test_write() {
        let data: i64 = 0;
        let mvcc = Mvcc::new(data);

        let read_txn = mvcc.read().await;
        assert_eq!(*read_txn, 0);

        {
            let mut write_txn = mvcc.write().await;
            {
                let mut_ptr = write_txn.get_mut();
                assert_eq!(*mut_ptr, 0);
                *mut_ptr = 1;
                assert_eq!(*mut_ptr, 1);
            }
            assert_eq!(*read_txn, 0);

            let another_read_txn = mvcc.read().await;
            assert_eq!(*another_read_txn, 0);
            // Commit write
            write_txn.commit().await;
        }

        /* Start a new txn and see it's still good */
        let yet_another_read_txn = mvcc.read().await;
        assert_eq!(*yet_another_read_txn, 1);
        assert_eq!(*read_txn, 0);

    }
}
