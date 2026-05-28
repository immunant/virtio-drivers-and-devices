//! Lock-free single-producer single-consumer ring buffer.
//!
//! The producer calls [`SpscRingBuffer::add`] and the consumer calls [`SpscRingBuffer::drain`].
//! These can safely be called concurrently from different threads without any locking, as long as
//! there is at most one producer and one consumer.

use alloc::boxed::Box;
use core::cmp::min;
use core::sync::atomic::{AtomicUsize, Ordering};
use zerocopy::FromZeros;

/// A lock-free single-producer single-consumer ring buffer.
///
/// The producer writes data with [`add`](Self::add) and the consumer reads data with
/// [`drain`](Self::drain). Concurrent use by one producer and one consumer is safe without locks.
pub struct SpscRingBuffer {
    buffer: Box<[u8]>,
    /// Index of the first used byte. Only modified by the consumer.
    head: AtomicUsize,
    /// Index one past the last used byte. Only modified by the producer.
    tail: AtomicUsize,
}

impl SpscRingBuffer {
    /// Creates a new ring buffer that can hold `capacity` bytes.
    ///
    /// Internally allocates `capacity + 1` bytes to distinguish full from empty.
    pub fn new(capacity: usize) -> Self {
        let alloc_size = capacity + 1;
        Self {
            buffer: FromZeros::new_box_zeroed_with_elems(alloc_size).unwrap(),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    /// Returns the total capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.buffer.len() - 1
    }

    /// Returns the number of bytes currently in the buffer.
    pub fn used(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        if tail >= head {
            tail - head
        } else {
            self.buffer.len() - head + tail
        }
    }

    /// Returns true iff there are currently no bytes in the buffer.
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
    }

    fn free(&self) -> usize {
        self.capacity() - self.used()
    }

    /// Adds the given bytes to the buffer if there is enough capacity for them all.
    ///
    /// Returns true if they were added, or false if there was not enough space.
    ///
    /// # Safety contract
    ///
    /// Must only be called by a single producer thread.
    pub fn add(&self, bytes: &[u8]) -> bool {
        if bytes.len() > self.free() {
            return false;
        }

        let tail = self.tail.load(Ordering::Relaxed);
        let len = self.buffer.len();

        let copy_before_wrap = min(bytes.len(), len - tail);
        // SAFETY: We are the sole producer. The consumer only reads from head..tail and we are
        // writing past tail. The free() check ensures we won't overwrite unread data.
        let buf = unsafe {
            core::slice::from_raw_parts_mut(self.buffer.as_ptr() as *mut u8, self.buffer.len())
        };
        buf[tail..tail + copy_before_wrap].copy_from_slice(&bytes[..copy_before_wrap]);
        if copy_before_wrap < bytes.len() {
            let remaining = bytes.len() - copy_before_wrap;
            buf[..remaining].copy_from_slice(&bytes[copy_before_wrap..]);
        }

        let new_tail = (tail + bytes.len()) % len;
        self.tail.store(new_tail, Ordering::Release);

        true
    }

    /// Reads and removes as many bytes as possible from the buffer, up to the length of the given
    /// output buffer.
    ///
    /// Returns the number of bytes read.
    ///
    /// # Safety contract
    ///
    /// Must only be called by a single consumer thread.
    pub fn drain(&self, out: &mut [u8]) -> usize {
        let available = self.used();
        let bytes_to_read = min(available, out.len());
        if bytes_to_read == 0 {
            return 0;
        }

        let head = self.head.load(Ordering::Relaxed);
        let len = self.buffer.len();

        let read_before_wrap = min(bytes_to_read, len - head);
        out[..read_before_wrap].copy_from_slice(&self.buffer[head..head + read_before_wrap]);
        if read_before_wrap < bytes_to_read {
            let remaining = bytes_to_read - read_before_wrap;
            out[read_before_wrap..bytes_to_read].copy_from_slice(&self.buffer[..remaining]);
        }

        let new_head = (head + bytes_to_read) % len;
        self.head.store(new_head, Ordering::Release);

        bytes_to_read
    }
}

// SAFETY: SpscRingBuffer is designed for concurrent use by one producer and one consumer.
// The atomic head/tail fields ensure proper synchronization.
unsafe impl Send for SpscRingBuffer {}
unsafe impl Sync for SpscRingBuffer {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn basic_add_drain() {
        let ring = SpscRingBuffer::new(16);
        assert!(ring.is_empty());
        assert_eq!(ring.capacity(), 16);

        assert!(ring.add(b"hello"));
        assert_eq!(ring.used(), 5);

        let mut buf = [0u8; 16];
        let n = ring.drain(&mut buf);
        assert_eq!(n, 5);
        assert_eq!(&buf[..5], b"hello");
        assert!(ring.is_empty());
    }

    #[test]
    fn wraparound() {
        let ring = SpscRingBuffer::new(8);
        // Fill most of the buffer
        assert!(ring.add(b"abcdef")); // 6 bytes
        let mut buf = [0u8; 4];
        ring.drain(&mut buf); // drain 4, head=4, tail=6
        assert_eq!(&buf, b"abcd");

        // Now add data that wraps around
        assert!(ring.add(b"ghijk")); // 5 bytes, wraps around
        let mut buf = [0u8; 7];
        let n = ring.drain(&mut buf);
        assert_eq!(n, 7);
        assert_eq!(&buf[..7], b"efghijk");
    }

    #[test]
    fn full_buffer_rejects() {
        let ring = SpscRingBuffer::new(4);
        assert!(ring.add(b"abcd"));
        assert!(!ring.add(b"e")); // full
        assert_eq!(ring.used(), 4);
    }

    #[test]
    fn concurrent_producer_consumer() {
        let ring = SpscRingBuffer::new(64);
        // SAFETY: This is safe because we use Arc and have exactly one producer and one consumer.
        let ring = alloc::sync::Arc::new(ring);
        let ring_producer = ring.clone();
        let ring_consumer = ring.clone();

        let producer = thread::spawn(move || {
            for i in 0u8..=255 {
                while !ring_producer.add(&[i]) {
                    core::hint::spin_loop();
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut received = alloc::vec::Vec::new();
            let mut buf = [0u8; 1];
            while received.len() < 256 {
                let n = ring_consumer.drain(&mut buf);
                if n > 0 {
                    received.push(buf[0]);
                } else {
                    core::hint::spin_loop();
                }
            }
            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();
        let expected: alloc::vec::Vec<u8> = (0u8..=255).collect();
        assert_eq!(received, expected);
    }
}
