#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(feature = "alloc")]
pub mod owning;

use crate::hal::{BufferDirection, DeviceDma, DeviceHal, Dma, DmaMemory, Hal, PhysAddr};
use crate::transport::{DeviceTransport, Transport};
use crate::{align_up, nonnull_slice_from_raw_parts, pages, Error, Result, PAGE_SIZE};
#[cfg(feature = "alloc")]
use alloc::boxed::Box;
#[cfg(feature = "alloc")]
use alloc::vec::Vec;
use bitflags::bitflags;
#[cfg(test)]
use core::cmp::min;
use core::convert::TryInto;
use core::hint::spin_loop;
use core::mem::{forget, size_of, take};
#[cfg(test)]
use core::ptr;
use core::ptr::NonNull;
use core::sync::atomic::{fence, AtomicU16, Ordering};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout};

/// The mechanism for bulk data transport on virtio devices.
///
/// Each device can have zero or more virtqueues.
///
/// * `SIZE`: The size of the queue. This is both the number of descriptors, and the number of slots
///   in the available and used rings. It must be a power of 2 and fit in a [`u16`].
#[derive(Debug)]
pub struct VirtQueue<H: Hal, const SIZE: usize> {
    /// DMA guard
    layout: VirtQueueLayout<Dma<H>>,
    /// Descriptor table
    ///
    /// The device may be able to modify this, even though it's not supposed to, so we shouldn't
    /// trust values read back from it. Use `desc_shadow` instead to keep track of what we wrote to
    /// it.
    desc: NonNull<[Descriptor]>,
    /// Available ring
    ///
    /// The device may be able to modify this, even though it's not supposed to, so we shouldn't
    /// trust values read back from it. The only field we need to read currently is `idx`, so we
    /// have `avail_idx` below to use instead.
    avail: NonNull<AvailRing<SIZE>>,
    /// Used ring
    used: NonNull<UsedRing<SIZE>>,

    /// The index of queue
    queue_idx: u16,
    /// The number of descriptors currently in use.
    num_used: u16,
    /// The head desc index of the free list.
    free_head: u16,
    /// Our trusted copy of `desc` that the device can't access.
    desc_shadow: [Descriptor; SIZE],
    /// Our trusted copy of `avail.idx`.
    avail_idx: u16,
    last_used_idx: u16,
    /// Whether the `VIRTIO_F_EVENT_IDX` feature has been negotiated.
    event_idx: bool,
    #[cfg(feature = "alloc")]
    indirect: bool,
    #[cfg(feature = "alloc")]
    indirect_lists: [Option<NonNull<[Descriptor]>>; SIZE],
}

impl<H: Hal, const SIZE: usize> VirtQueue<H, SIZE> {
    const SIZE_OK: () = assert!(SIZE.is_power_of_two() && SIZE <= u16::MAX as usize);

    /// Creates a new VirtQueue.
    ///
    /// * `indirect`: Whether to use indirect descriptors. This should be set if the
    ///   `VIRTIO_F_INDIRECT_DESC` feature has been negotiated with the device.
    /// * `event_idx`: Whether to use the `used_event` and `avail_event` fields for notification
    ///   suppression. This should be set if the `VIRTIO_F_EVENT_IDX` feature has been negotiated
    ///   with the device.
    pub fn new<T: Transport>(
        transport: &mut T,
        idx: u16,
        indirect: bool,
        event_idx: bool,
    ) -> Result<Self> {
        #[allow(clippy::let_unit_value)]
        let _ = Self::SIZE_OK;

        if transport.queue_used(idx) {
            return Err(Error::AlreadyUsed);
        }
        if transport.max_queue_size(idx) < SIZE as u32 {
            return Err(Error::InvalidParam);
        }
        let size = SIZE as u16;

        let layout = if transport.requires_legacy_layout() {
            VirtQueueLayout::allocate_legacy(size)?
        } else {
            VirtQueueLayout::allocate_flexible(size)?
        };

        transport.queue_set(
            idx,
            size.into(),
            layout.descriptors_paddr(),
            layout.driver_area_paddr(),
            layout.device_area_paddr(),
        );

        let desc =
            nonnull_slice_from_raw_parts(layout.descriptors_vaddr().cast::<Descriptor>(), SIZE);
        let avail = layout.avail_vaddr().cast();
        let used = layout.used_vaddr().cast();

        let mut desc_shadow: [Descriptor; SIZE] = FromZeros::new_zeroed();
        // Link descriptors together.
        for i in 0..(size - 1) {
            desc_shadow[i as usize].next = i + 1;
            // SAFETY: `desc` is properly aligned, dereferenceable, initialised,
            // and the device won't access the descriptors for the duration of this unsafe block.
            unsafe {
                (*desc.as_ptr())[i as usize].next = i + 1;
            }
        }

        #[cfg(feature = "alloc")]
        const NONE: Option<NonNull<[Descriptor]>> = None;
        Ok(VirtQueue {
            layout,
            desc,
            avail,
            used,
            queue_idx: idx,
            num_used: 0,
            free_head: 0,
            desc_shadow,
            avail_idx: 0,
            last_used_idx: 0,
            event_idx,
            #[cfg(feature = "alloc")]
            indirect,
            #[cfg(feature = "alloc")]
            indirect_lists: [NONE; SIZE],
        })
    }

    /// Add buffers to the virtqueue, return a token.
    ///
    /// The buffers must not be empty.
    ///
    /// Ref: linux virtio_ring.c virtqueue_add
    ///
    /// # Safety
    ///
    /// The input and output buffers must remain valid and not be accessed until a call to
    /// `pop_used` with the returned token succeeds.
    pub unsafe fn add<'a, 'b>(
        &mut self,
        inputs: &'a [&'b [u8]],
        outputs: &'a mut [&'b mut [u8]],
    ) -> Result<u16> {
        if inputs.is_empty() && outputs.is_empty() {
            return Err(Error::InvalidParam);
        }
        let descriptors_needed = inputs.len() + outputs.len();
        // Only consider indirect descriptors if the alloc feature is enabled, as they require
        // allocation.
        #[cfg(feature = "alloc")]
        if self.num_used as usize + 1 > SIZE
            || descriptors_needed > SIZE
            || (!self.indirect && self.num_used as usize + descriptors_needed > SIZE)
        {
            return Err(Error::QueueFull);
        }
        #[cfg(not(feature = "alloc"))]
        if self.num_used as usize + descriptors_needed > SIZE {
            return Err(Error::QueueFull);
        }

        #[cfg(feature = "alloc")]
        let head = if self.indirect && descriptors_needed > 1 {
            self.add_indirect(inputs, outputs)
        } else {
            self.add_direct(inputs, outputs)
        };
        #[cfg(not(feature = "alloc"))]
        let head = self.add_direct(inputs, outputs);

        let avail_slot = self.avail_idx & (SIZE as u16 - 1);
        // SAFETY: `self.avail` is properly aligned, dereferenceable and initialised.
        unsafe {
            (*self.avail.as_ptr()).ring[avail_slot as usize] = head;
        }

        // Write barrier so that device sees changes to descriptor table and available ring before
        // change to available index.
        fence(Ordering::SeqCst);

        // increase head of avail ring
        self.avail_idx = self.avail_idx.wrapping_add(1);
        // SAFETY: `self.avail` is properly aligned, dereferenceable and initialised.
        unsafe {
            (*self.avail.as_ptr())
                .idx
                .store(self.avail_idx, Ordering::Release);
        }

        Ok(head)
    }

    fn add_direct<'a, 'b>(
        &mut self,
        inputs: &'a [&'b [u8]],
        outputs: &'a mut [&'b mut [u8]],
    ) -> u16 {
        // allocate descriptors from free list
        let head = self.free_head;
        let mut last = self.free_head;

        for (buffer, direction) in InputOutputIter::new(inputs, outputs) {
            assert_ne!(buffer.len(), 0);

            // Write to desc_shadow then copy.
            let desc = &mut self.desc_shadow[usize::from(self.free_head)];
            // SAFETY: Our caller promises that the buffers live at least until `pop_used`
            // returns them.
            unsafe {
                desc.set_buf::<H>(buffer, direction, DescFlags::NEXT);
            }
            last = self.free_head;
            self.free_head = desc.next;

            self.write_desc(last);
        }

        // set last_elem.next = NULL
        self.desc_shadow[usize::from(last)]
            .flags
            .remove(DescFlags::NEXT);
        self.write_desc(last);

        self.num_used += (inputs.len() + outputs.len()) as u16;

        head
    }

    #[cfg(feature = "alloc")]
    fn add_indirect<'a, 'b>(
        &mut self,
        inputs: &'a [&'b [u8]],
        outputs: &'a mut [&'b mut [u8]],
    ) -> u16 {
        let head = self.free_head;

        // Allocate and fill in indirect descriptor list.
        let mut indirect_list =
            <[Descriptor]>::new_box_zeroed_with_elems(inputs.len() + outputs.len()).unwrap();
        for (i, (buffer, direction)) in InputOutputIter::new(inputs, outputs).enumerate() {
            let desc = &mut indirect_list[i];
            // SAFETY: Our caller promises that the buffers live at least until `pop_used`
            // returns them.
            unsafe {
                desc.set_buf::<H>(buffer, direction, DescFlags::NEXT);
            }
            desc.next = (i + 1) as u16;
        }
        indirect_list
            .last_mut()
            .unwrap()
            .flags
            .remove(DescFlags::NEXT);

        // Need to store pointer to indirect_list too, because direct_desc.set_buf will only store
        // the physical DMA address which might be different.
        assert!(self.indirect_lists[usize::from(head)].is_none());
        self.indirect_lists[usize::from(head)] = Some(indirect_list.as_mut().into());

        // Write a descriptor pointing to indirect descriptor list. We use Box::leak to prevent the
        // indirect list from being freed when this function returns; recycle_descriptors is instead
        // responsible for freeing the memory after the buffer chain is popped.
        let direct_desc = &mut self.desc_shadow[usize::from(head)];
        self.free_head = direct_desc.next;

        // SAFETY: Using `Box::leak` on `indirect_list` guarantees it won't be deallocated
        // when this function returns. The allocation isn't freed until
        // `recycle_descriptors` is called, at which point the allocation is no longer being
        // used.
        unsafe {
            direct_desc.set_buf::<H>(
                Box::leak(indirect_list).as_bytes().into(),
                BufferDirection::DriverToDevice,
                DescFlags::INDIRECT,
            );
        }
        self.write_desc(head);
        self.num_used += 1;

        head
    }

    /// Add the given buffers to the virtqueue, notifies the device, blocks until the device uses
    /// them, then pops them.
    ///
    /// This assumes that the device isn't processing any other buffers at the same time.
    ///
    /// The buffers must not be empty.
    pub fn add_notify_wait_pop<'a>(
        &mut self,
        inputs: &'a [&'a [u8]],
        outputs: &'a mut [&'a mut [u8]],
        transport: &mut impl Transport,
    ) -> Result<u32> {
        // SAFETY: We don't return until the same token has been popped, so the buffers remain
        // valid and are not otherwise accessed until then.
        let token = unsafe { self.add(inputs, outputs) }?;

        // Notify the queue.
        if self.should_notify() {
            transport.notify(self.queue_idx);
        }

        // Wait until there is at least one element in the used ring.
        while !self.can_pop() {
            spin_loop();
        }

        // SAFETY: These are the same buffers as we passed to `add` above and they are still valid.
        unsafe { self.pop_used(token, inputs, outputs) }
    }

    /// Advise the device whether used buffer notifications are needed.
    ///
    /// See Virtio v1.1 2.6.7 Used Buffer Notification Suppression
    pub fn set_dev_notify(&mut self, enable: bool) {
        let avail_ring_flags = if enable { 0x0000 } else { 0x0001 };
        if !self.event_idx {
            // SAFETY: `self.avail` points to a valid, aligned, initialised, dereferenceable, readable
            // instance of `AvailRing`.
            unsafe {
                (*self.avail.as_ptr())
                    .flags
                    .store(avail_ring_flags, Ordering::Release)
            }
        }
    }

    /// Returns whether the driver should notify the device after adding a new buffer to the
    /// virtqueue.
    ///
    /// This will be false if the device has supressed notifications.
    pub fn should_notify(&self) -> bool {
        if self.event_idx {
            // SAFETY: `self.used` points to a valid, aligned, initialised, dereferenceable, readable
            // instance of `UsedRing`.
            let avail_event = unsafe { (*self.used.as_ptr()).avail_event.load(Ordering::Acquire) };
            self.avail_idx >= avail_event.wrapping_add(1)
        } else {
            // SAFETY: `self.used` points to a valid, aligned, initialised, dereferenceable, readable
            // instance of `UsedRing`.
            unsafe { (*self.used.as_ptr()).flags.load(Ordering::Acquire) & 0x0001 == 0 }
        }
    }

    /// Copies the descriptor at the given index from `desc_shadow` to `desc`, so it can be seen by
    /// the device.
    fn write_desc(&mut self, index: u16) {
        let index = usize::from(index);
        // SAFETY: `self.desc` is properly aligned, dereferenceable and initialised, and nothing
        // else reads or writes the descriptor during this block.
        unsafe {
            (*self.desc.as_ptr())[index] = self.desc_shadow[index].clone();
        }
    }

    /// Returns whether there is a used element that can be popped.
    pub fn can_pop(&self) -> bool {
        // SAFETY: `self.used` points to a valid, aligned, initialised, dereferenceable, readable
        // instance of `UsedRing`.
        self.last_used_idx != unsafe { (*self.used.as_ptr()).idx.load(Ordering::Acquire) }
    }

    /// Returns the descriptor index (a.k.a. token) of the next used element without popping it, or
    /// `None` if the used ring is empty.
    pub fn peek_used(&self) -> Option<u16> {
        if self.can_pop() {
            let last_used_slot = self.last_used_idx & (SIZE as u16 - 1);
            // SAFETY: `self.used` points to a valid, aligned, initialised, dereferenceable,
            // readable instance of `UsedRing`.
            Some(unsafe { (*self.used.as_ptr()).ring[last_used_slot as usize].id as u16 })
        } else {
            None
        }
    }

    /// Returns the number of free descriptors.
    pub fn available_desc(&self) -> usize {
        #[cfg(feature = "alloc")]
        if self.indirect {
            return if usize::from(self.num_used) == SIZE {
                0
            } else {
                SIZE
            };
        }

        SIZE - usize::from(self.num_used)
    }

    /// Unshares buffers in the list starting at descriptor index `head` and adds them to the free
    /// list. Unsharing may involve copying data back to the original buffers, so they must be
    /// passed in too.
    ///
    /// This will push all linked descriptors at the front of the free list.
    ///
    /// # Safety
    ///
    /// The buffers in `inputs` and `outputs` must match the set of buffers originally added to the
    /// queue by `add`.
    unsafe fn recycle_descriptors<'a>(
        &mut self,
        head: u16,
        inputs: &'a [&'a [u8]],
        outputs: &'a mut [&'a mut [u8]],
    ) {
        let original_free_head = self.free_head;
        self.free_head = head;

        let head_desc = &mut self.desc_shadow[usize::from(head)];
        if head_desc.flags.contains(DescFlags::INDIRECT) {
            #[cfg(feature = "alloc")]
            {
                // Find the indirect descriptor list, unshare it and move its descriptor to the free
                // list.
                let indirect_list = self.indirect_lists[usize::from(head)].take().unwrap();
                // SAFETY: We allocated the indirect list in `add_indirect`, and the device has
                // finished accessing it by this point.
                let mut indirect_list = unsafe { Box::from_raw(indirect_list.as_ptr()) };
                let paddr = head_desc.addr;
                head_desc.unset_buf();
                self.num_used -= 1;
                head_desc.next = original_free_head;

                // SAFETY: `paddr` comes from a previous call `H::share` (inside
                // `Descriptor::set_buf`, which was called from `add_direct` or `add_indirect`).
                // `indirect_list` is owned by this function and is not accessed from any other threads.
                unsafe {
                    H::unshare(
                        paddr as usize,
                        indirect_list.as_mut_bytes().into(),
                        BufferDirection::DriverToDevice,
                    );
                }

                // Unshare the buffers in the indirect descriptor list, and free it.
                assert_eq!(indirect_list.len(), inputs.len() + outputs.len());
                for (i, (buffer, direction)) in InputOutputIter::new(inputs, outputs).enumerate() {
                    assert_ne!(buffer.len(), 0);

                    // SAFETY: The caller ensures that the buffer is valid and matches the
                    // descriptor from which we got `paddr`.
                    unsafe {
                        // Unshare the buffer (and perhaps copy its contents back to the original
                        // buffer).
                        H::unshare(indirect_list[i].addr as usize, buffer, direction);
                    }
                }
                drop(indirect_list);
            }
        } else {
            let mut next = Some(head);

            for (buffer, direction) in InputOutputIter::new(inputs, outputs) {
                assert_ne!(buffer.len(), 0);

                let desc_index = next.expect("Descriptor chain was shorter than expected.");
                let desc = &mut self.desc_shadow[usize::from(desc_index)];

                let paddr = desc.addr;
                desc.unset_buf();
                self.num_used -= 1;
                next = desc.next();
                if next.is_none() {
                    desc.next = original_free_head;
                }

                self.write_desc(desc_index);

                // SAFETY: The caller ensures that the buffer is valid and matches the descriptor
                // from which we got `paddr`.
                unsafe {
                    // Unshare the buffer (and perhaps copy its contents back to the original buffer).
                    H::unshare(paddr as usize, buffer, direction);
                }
            }

            if next.is_some() {
                panic!("Descriptor chain was longer than expected.");
            }
        }
    }

    /// If the given token is next on the device used queue, pops it and returns the total buffer
    /// length which was used (written) by the device.
    ///
    /// Ref: linux virtio_ring.c virtqueue_get_buf_ctx
    ///
    /// # Safety
    ///
    /// The buffers in `inputs` and `outputs` must match the set of buffers originally added to the
    /// queue by `add` when it returned the token being passed in here.
    pub unsafe fn pop_used<'a>(
        &mut self,
        token: u16,
        inputs: &'a [&'a [u8]],
        outputs: &'a mut [&'a mut [u8]],
    ) -> Result<u32> {
        if !self.can_pop() {
            return Err(Error::NotReady);
        }

        // Get the index of the start of the descriptor chain for the next element in the used ring.
        let last_used_slot = self.last_used_idx & (SIZE as u16 - 1);
        let index;
        let len;
        // SAFETY: `self.used` points to a valid, aligned, initialised, dereferenceable, readable
        // instance of `UsedRing`.
        unsafe {
            index = (*self.used.as_ptr()).ring[last_used_slot as usize].id as u16;
            len = (*self.used.as_ptr()).ring[last_used_slot as usize].len;
        }

        if index != token {
            // The device used a different descriptor chain to the one we were expecting.
            return Err(Error::WrongToken);
        }

        // SAFETY: The caller ensures the buffers are valid and match the descriptor.
        unsafe {
            self.recycle_descriptors(index, inputs, outputs);
        }
        self.last_used_idx = self.last_used_idx.wrapping_add(1);

        if self.event_idx {
            // SAFETY: `self.avail` points to a valid, aligned, initialised, dereferenceable,
            // readable instance of `AvailRing`.
            unsafe {
                (*self.avail.as_ptr())
                    .used_event
                    .store(self.last_used_idx, Ordering::Release);
            }
        }

        Ok(len)
    }
}

// SAFETY: None of the virt queue resources are tied to a particular thread.
unsafe impl<H: Hal, const SIZE: usize> Send for VirtQueue<H, SIZE> {}

// SAFETY: A `&VirtQueue` only allows reading from the various pointers it contains, so there is no
// data race.
unsafe impl<H: Hal, const SIZE: usize> Sync for VirtQueue<H, SIZE> {}

#[derive(Debug)]
pub struct MappedDescriptor<H: DeviceHal> {
    desc_copy: Descriptor,
    dma: DeviceDma<H>,
}

impl<H: DeviceHal> PartialEq for MappedDescriptor<H> {
    fn eq(&self, other: &Self) -> bool {
        (self.desc_copy == other.desc_copy) && (self.dma == other.dma)
    }
}

impl<H: DeviceHal> MappedDescriptor<H> {
    // SAFETY: The caller must ensure that the entire chain of buffers described by desc_copy came
    // from a device virtqueue descriptor.
    unsafe fn map_buf(desc_copy: Descriptor, client_id: u16) -> Result<Self> {
        let direction = if desc_copy.flags.contains(DescFlags::WRITE) {
            BufferDirection::DeviceToDriver
        } else {
            BufferDirection::DriverToDevice
        };
        // SAFETY: The safety requirements on this function ensure that the memory region can be
        // mapped in as DMA memory.
        let dma = unsafe {
            DeviceDma::new(
                desc_copy.addr as PhysAddr,
                pages(desc_copy.len as usize),
                direction,
                client_id,
            )?
        };
        Ok(Self { desc_copy, dma })
    }
}

#[cfg(feature = "alloc")]
#[derive(Debug)]
struct DescriptorBuffers<'a> {
    read_buffers: Vec<&'a [u8]>,
    write_buffers: Vec<&'a mut [u8]>,
    head: u16,
}

#[derive(Debug)]
pub struct DeviceVirtQueue<H: DeviceHal, const SIZE: usize> {
    /// DMA guard
    layout: VirtQueueLayout<DeviceDma<H>>,

    desc: NonNull<[Descriptor]>,
    avail: NonNull<AvailRing<SIZE>>,
    used: NonNull<UsedRing<SIZE>>,

    queue_idx: u16,

    /// Our trusted copy of `avail.idx`.
    avail_idx: u16,
    last_used_idx: u16,
    desc_mapped: [Option<MappedDescriptor<H>>; SIZE],
    client_id: u16,
}

impl<H: DeviceHal, const SIZE: usize> DeviceVirtQueue<H, SIZE> {
    const SIZE_OK: () = assert!(SIZE.is_power_of_two() && SIZE <= u16::MAX as usize);

    pub fn new<T: DeviceTransport>(transport: &mut T, idx: u16) -> Result<Self> {
        #[allow(clippy::let_unit_value)]
        let _ = Self::SIZE_OK;

        if transport.max_queue_size(idx) < SIZE as u32 {
            return Err(Error::InvalidParam);
        }
        let client_id = transport.get_client_id();

        let size = SIZE as u16;

        let [paddr, _, used_paddr] = transport.queue_get(idx);

        let layout = if transport.requires_legacy_layout() {
            // SAFETY: paddr was the physical address returned by the DeviceTransport implementor
            // for the start of the virtqueue (i.e. descriptor table)
            unsafe { VirtQueueLayout::map_legacy(size, paddr, client_id)? }
        } else {
            // SAFETY: paddr was the physical address returned by the DeviceTransport implementor
            // for the start of the virtqueue. used_paddr was the physical address returned for the
            // used vring.
            unsafe { VirtQueueLayout::map_flexible(size, paddr, used_paddr, client_id)? }
        };
        let desc =
            nonnull_slice_from_raw_parts(layout.descriptors_vaddr().cast::<Descriptor>(), SIZE);
        let avail = layout.avail_vaddr().cast();
        let used = layout.used_vaddr().cast();
        let desc_mapped = [const { None }; SIZE];
        Ok(DeviceVirtQueue {
            layout,
            desc,
            avail,
            used,
            queue_idx: idx,
            avail_idx: 0,
            last_used_idx: 0,
            desc_mapped,
            client_id,
        })
    }

    pub fn wait_pop_add_notify(
        &mut self,
        inputs: &[&[u8]],
        transport: &mut impl DeviceTransport,
    ) -> Result<()> {
        #[cfg(feature = "alloc")]
        {
            while !self.can_pop() {
                spin_loop();
            }
            // SAFETY: inputs is copied into the first write buffer then they are returned to the
            // used vring and not accessed again. This function waits until it can pop the avail
            // vring so this should never panic
            let mut popped = unsafe { self.pop_avail()?.unwrap() };

            // If there isn't at least one write buffer, the device isn't ready
            if popped.write_buffers.is_empty() {
                return Err(Error::NotReady);
            }

            // A mix of write and read buffers is currently not supported
            // TODO: Support popping chains of mixed descriptors by caching any read buffers popped
            // here.
            if !popped.read_buffers.is_empty() {
                return Err(Error::Unsupported);
            }

            let out_buf = &mut popped.write_buffers[0];
            let mut copied = 0;
            for in_buf in inputs {
                out_buf[copied..copied + in_buf.len()].copy_from_slice(in_buf);
                copied += in_buf.len();
            }

            let head_len = copied;
            // Return the entire popped chain by writing the head to the used vring
            self.add_used(popped.head, head_len);

            if self.should_notify() {
                transport.notify(self.queue_idx);
            }
            Ok(())
        }
        #[cfg(not(feature = "alloc"))]
        unreachable!("device virtqueue send loop requires alloc feature")
    }

    pub fn poll<T>(
        &mut self,
        transport: &mut impl DeviceTransport,
        handler: impl FnOnce(&[u8]) -> Result<Option<T>>,
    ) -> Result<Option<T>> {
        #[cfg(feature = "alloc")]
        {
            // TODO: Store any popped write buffers to avoid potential deadlocks caused by mixed
            // descriptor chains.
            // SAFETY: The buffers are copied to a single temporary buffer. Then handler is called
            // on that and the original buffers are returned to the used vring and not accessed again.
            let Some(popped) = (unsafe { self.pop_avail()? }) else {
                return Ok(None);
            };

            // A mix of write and read buffers is currently not supported
            // TODO: Support popping chains of mixed descriptors by caching any write buffers popped
            // here.
            if !popped.write_buffers.is_empty() {
                return Err(Error::Unsupported);
            }

            let mut tmp = Vec::new();
            for in_buf in &popped.read_buffers {
                tmp.extend_from_slice(in_buf);
            }
            let result = handler(tmp.as_slice());

            self.add_used(
                popped.head,
                0, /* zero bytes were written to the write buffers */
            );

            if self.should_notify() {
                transport.notify(self.queue_idx);
            }
            result
        }
        #[cfg(not(feature = "alloc"))]
        unreachable!("device virtqueue polling requires alloc feature")
    }

    fn add_used(&mut self, head: u16, head_len: usize) {
        let last_used_slot = self.last_used_idx & (SIZE as u16 - 1);
        // SAFETY: self.used is properly aligned, dereferenceable and initialised instance of
        // UsedRing
        unsafe {
            (*self.used.as_ptr()).ring[usize::from(last_used_slot)].id = u32::from(head);
            (*self.used.as_ptr()).ring[usize::from(last_used_slot)].len = head_len as u32;
        }

        fence(Ordering::SeqCst);

        self.last_used_idx = self.last_used_idx.wrapping_add(1);
        // SAFETY: self.used is properly aligned, dereferenceable and initialised instance of
        // UsedRing
        unsafe {
            (*self.used.as_ptr())
                .idx
                .store(self.last_used_idx, Ordering::Release);
        }
    }

    fn read_desc(&mut self, index: u16) -> Result<Descriptor> {
        let index = usize::from(index);
        // SAFETY: self.desc is a properly aligned, dereferencable and initialised instance of
        // Descriptor
        let desc = unsafe { (*self.desc.as_ptr()).get(index) };
        desc.ok_or(Error::WrongToken).cloned()
    }

    /// Pop a chain of buffers from the avail vring and return the index of the first buffer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the returned buffers are not accessed after the first buffer's
    /// token has been written to the used vring and the `last_used` index has been updated.
    #[cfg(feature = "alloc")]
    unsafe fn pop_avail<'a>(&mut self) -> Result<Option<DescriptorBuffers<'a>>> {
        let Some(head) = self.peek_avail() else {
            return Ok(None);
        };
        let mut read_buffers = Vec::new();
        let mut write_buffers = Vec::new();
        let mut next_token = Some(head);
        while let Some(token) = next_token {
            let desc = self.read_desc(token)?;
            let avail_len = desc.len as usize;
            let write = desc.flags.contains(DescFlags::WRITE);
            assert!(!desc.flags.contains(DescFlags::INDIRECT));
            next_token = if desc.flags.contains(DescFlags::NEXT) {
                Some(desc.next)
            } else {
                None
            };
            // Check if a buffer has previously been mapped in for this descriptor entry
            let mapped_desc = self
                .desc_mapped
                .get_mut(usize::from(token))
                .ok_or(Error::WrongToken)?;

            // SAFETY: desc was read from the virtqueue descriptor table and is currently not in
            // use since it was either obtained by getting the next available index from
            // peek_avail and using that to index into the descriptor table or through a chain
            // of buffers starting from the buffer obtained via peek_avail.
            let new_desc = unsafe { MappedDescriptor::map_buf(desc, self.client_id)? };

            let desc_buf_changed = if let Some(prev_mapped_desc) = mapped_desc {
                // If there was already a mapped descriptor compare both the physical and virtual
                // addresses against the new descriptor. We cannot only compare the physical
                // addresses because if they're translated (e.g. if VIRTIO_F_ACCESS_PLATFORM is
                // used) the bus addresses in the descriptors' address fields may be translated to
                // different physical addresses even if the descriptor itself hasn't changed.
                *prev_mapped_desc != new_desc
            } else {
                true
            };
            if desc_buf_changed {
                // Store the newly mapped dscriptor buffer new_desc into self.desc_mapped[token],
                // dropping the old MappedDescriptor if any and unmapping the old descriptor buffer.
                *mapped_desc = Some(new_desc);
            } else {
                // If the descriptor and its buffer didn't change drop `new_desc` since the DMA
                // memory will be unmapped when self.desc_mapped[token] is dropped or replaced.
                forget(new_desc);
            }
            let mut buffer = mapped_desc.as_ref().unwrap().dma.raw_slice();
            if write {
                // SAFETY: Safety delegated to safety requirements on this function.
                let buffer = unsafe { &mut buffer.as_mut()[0..avail_len] };
                write_buffers.push(buffer);
            } else {
                // All read descriptors must come before write descriptors so if we've seen any
                // write descriptors error out.
                if !write_buffers.is_empty() {
                    return Err(Error::InvalidDescriptor);
                }
                // SAFETY: Safety delegated to safety requirements on this function.
                let buffer = unsafe { &buffer.as_ref()[0..avail_len] };
                read_buffers.push(buffer);
            }
        }
        self.avail_idx = self.avail_idx.wrapping_add(1);
        Ok(Some(DescriptorBuffers {
            read_buffers,
            write_buffers,
            head,
        }))
    }

    fn can_pop(&self) -> bool {
        // SAFETY: self.avail points to a valid, aligned, initialised, dereferenceable, readable
        // instance of AvailRing.
        self.avail_idx != unsafe { (*self.avail.as_ptr()).idx.load(Ordering::Acquire) }
    }

    fn peek_avail(&self) -> Option<u16> {
        if self.can_pop() {
            let avail_slot = self.avail_idx & (SIZE as u16 - 1);
            // SAFETY: self.avail points to a valid, aligned, initialised, dereferenceable,
            // readable instance of AvailRing.
            Some(unsafe { (*self.avail.as_ptr()).ring[avail_slot as usize] })
        } else {
            None
        }
    }

    fn should_notify(&self) -> bool {
        // SAFETY: self.avail points to a valid, aligned, initialised, dereferenceable, readable
        // instance of AvailRing.
        unsafe { (*self.avail.as_ptr()).flags.load(Ordering::Acquire) & 0x0001 == 0 }
    }
}

// SAFETY: None of the virt queue resources are tied to a particular thread.
unsafe impl<H: DeviceHal, const SIZE: usize> Send for DeviceVirtQueue<H, SIZE> {}

// SAFETY: A `&DeviceVirtQueue` only allows reading from the various pointers it contains, so there is no
// data race.
unsafe impl<H: DeviceHal, const SIZE: usize> Sync for DeviceVirtQueue<H, SIZE> {}

/// The inner layout of a VirtQueue.
///
/// Ref: 2.6 Split Virtqueues
#[derive(Debug)]
enum VirtQueueLayout<D: DmaMemory> {
    Legacy {
        dma: D,
        avail_offset: usize,
        used_offset: usize,
    },
    Modern {
        /// The region used for the descriptor area and driver area.
        driver_to_device_dma: D,
        /// The region used for the device area.
        device_to_driver_dma: D,
        /// The offset from the start of the `driver_to_device_dma` region to the driver area
        /// (available ring).
        avail_offset: usize,
    },
}

impl<H: Hal> VirtQueueLayout<Dma<H>> {
    /// Allocates a single DMA region containing all parts of the virtqueue, following the layout
    /// required by legacy interfaces.
    ///
    /// Ref: 2.6.2 Legacy Interfaces: A Note on Virtqueue Layout
    fn allocate_legacy(queue_size: u16) -> Result<Self> {
        let (desc, avail, used) = queue_part_sizes(queue_size);
        let size = align_up(desc + avail) + align_up(used);
        // Allocate contiguous pages.
        let dma = Dma::new(size / PAGE_SIZE, BufferDirection::Both)?;
        Ok(Self::Legacy {
            dma,
            avail_offset: desc,
            used_offset: align_up(desc + avail),
        })
    }

    /// Allocates separate DMA regions for the the different parts of the virtqueue, as supported by
    /// non-legacy interfaces.
    ///
    /// This is preferred over `allocate_legacy` where possible as it reduces memory fragmentation
    /// and allows the HAL to know which DMA regions are used in which direction.
    fn allocate_flexible(queue_size: u16) -> Result<Self> {
        let (desc, avail, used) = queue_part_sizes(queue_size);
        let driver_to_device_dma = Dma::new(pages(desc + avail), BufferDirection::DriverToDevice)?;
        let device_to_driver_dma = Dma::new(pages(used), BufferDirection::DeviceToDriver)?;
        Ok(Self::Modern {
            driver_to_device_dma,
            device_to_driver_dma,
            avail_offset: desc,
        })
    }
}

impl<H: DeviceHal> VirtQueueLayout<DeviceDma<H>> {
    // SAFETY: paddr must be memory shared by a virtio driver for a split virtqueue with the legacy
    // layout and queue_size entries.
    unsafe fn map_legacy(queue_size: u16, paddr: PhysAddr, client_id: u16) -> Result<Self> {
        let (desc, avail, used) = queue_part_sizes(queue_size);
        let size = align_up(desc + avail) + align_up(used);
        // SAFETY: The safety requirements on this function ensure that this memory region can be
        // mapped in as DMA memory.
        let dma =
            unsafe { DeviceDma::new(paddr, size / PAGE_SIZE, BufferDirection::Both, client_id)? };
        Ok(Self::Legacy {
            dma,
            avail_offset: desc,
            used_offset: align_up(desc + avail),
        })
    }

    // SAFETY: desc_avail_paddr and used_paddr must be memory shared by a virtio driver for a split
    // virtqueue where the device writeable and driver writeable portions are described by separate
    // memory regions. Specifically desc_avail_paddr must point to the descriptor table and
    // available vring and used_paddr must point to the used vring.
    unsafe fn map_flexible(
        queue_size: u16,
        desc_avail_paddr: PhysAddr,
        used_paddr: PhysAddr,
        client_id: u16,
    ) -> Result<Self> {
        let (desc, avail, used) = queue_part_sizes(queue_size);
        // SAFETY: The safety requirements on this function ensure that this memory region can be
        // mapped in as DMA memory.
        let driver_to_device_dma = unsafe {
            DeviceDma::new(
                desc_avail_paddr,
                pages(desc + avail),
                BufferDirection::DriverToDevice,
                client_id,
            )?
        };
        // SAFETY: The safety requirements on this function ensure that this memory region can be
        // mapped in as DMA memory.
        let device_to_driver_dma = unsafe {
            DeviceDma::new(
                used_paddr,
                pages(used),
                BufferDirection::DeviceToDriver,
                client_id,
            )?
        };
        Ok(Self::Modern {
            driver_to_device_dma,
            device_to_driver_dma,
            avail_offset: desc,
        })
    }
}

impl<D: DmaMemory> VirtQueueLayout<D> {
    /// Returns the physical address of the descriptor area.
    fn descriptors_paddr(&self) -> PhysAddr {
        match self {
            Self::Legacy { dma, .. } => dma.paddr(),
            Self::Modern {
                driver_to_device_dma,
                ..
            } => driver_to_device_dma.paddr(),
        }
    }

    /// Returns a pointer to the descriptor table (in the descriptor area).
    fn descriptors_vaddr(&self) -> NonNull<u8> {
        match self {
            Self::Legacy { dma, .. } => dma.vaddr(0),
            Self::Modern {
                driver_to_device_dma,
                ..
            } => driver_to_device_dma.vaddr(0),
        }
    }

    /// Returns the physical address of the driver area.
    fn driver_area_paddr(&self) -> PhysAddr {
        match self {
            Self::Legacy {
                dma, avail_offset, ..
            } => dma.paddr() + avail_offset,
            Self::Modern {
                driver_to_device_dma,
                avail_offset,
                ..
            } => driver_to_device_dma.paddr() + avail_offset,
        }
    }

    /// Returns a pointer to the available ring (in the driver area).
    fn avail_vaddr(&self) -> NonNull<u8> {
        match self {
            Self::Legacy {
                dma, avail_offset, ..
            } => dma.vaddr(*avail_offset),
            Self::Modern {
                driver_to_device_dma,
                avail_offset,
                ..
            } => driver_to_device_dma.vaddr(*avail_offset),
        }
    }

    /// Returns the physical address of the device area.
    fn device_area_paddr(&self) -> PhysAddr {
        match self {
            Self::Legacy {
                used_offset, dma, ..
            } => dma.paddr() + used_offset,
            Self::Modern {
                device_to_driver_dma,
                ..
            } => device_to_driver_dma.paddr(),
        }
    }

    /// Returns a pointer to the used ring (in the driver area).
    fn used_vaddr(&self) -> NonNull<u8> {
        match self {
            Self::Legacy {
                dma, used_offset, ..
            } => dma.vaddr(*used_offset),
            Self::Modern {
                device_to_driver_dma,
                ..
            } => device_to_driver_dma.vaddr(0),
        }
    }
}

/// Returns the size in bytes of the descriptor table, available ring and used ring for a given
/// queue size.
///
/// Ref: 2.6 Split Virtqueues
fn queue_part_sizes(queue_size: u16) -> (usize, usize, usize) {
    assert!(
        queue_size.is_power_of_two(),
        "queue size should be a power of 2"
    );
    let queue_size = queue_size as usize;
    let desc = size_of::<Descriptor>() * queue_size;
    let avail = size_of::<u16>() * (3 + queue_size);
    let used = size_of::<u16>() * 3 + size_of::<UsedElem>() * queue_size;
    (desc, avail, used)
}

#[repr(C, align(16))]
#[derive(Clone, Debug, FromBytes, Immutable, IntoBytes, KnownLayout, PartialEq)]
pub(crate) struct Descriptor {
    addr: u64,
    len: u32,
    flags: DescFlags,
    next: u16,
}

impl Descriptor {
    /// Sets the buffer address, length and flags, and shares it with the device.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the buffer lives at least as long as the descriptor is active.
    unsafe fn set_buf<H: Hal>(
        &mut self,
        buf: NonNull<[u8]>,
        direction: BufferDirection,
        extra_flags: DescFlags,
    ) {
        // SAFETY: Our caller promises that the buffer is valid.
        unsafe {
            self.addr = H::share(buf, direction) as u64;
        }
        self.len = buf.len().try_into().unwrap();
        self.flags = extra_flags
            | match direction {
                BufferDirection::DeviceToDriver => DescFlags::WRITE,
                BufferDirection::DriverToDevice => DescFlags::empty(),
                BufferDirection::Both => {
                    panic!("Buffer passed to device should never use BufferDirection::Both.")
                }
            };
    }

    /// Sets the buffer address and length to 0.
    ///
    /// This must only be called once the device has finished using the descriptor.
    fn unset_buf(&mut self) {
        self.addr = 0;
        self.len = 0;
    }

    /// Returns the index of the next descriptor in the chain if the `NEXT` flag is set, or `None`
    /// if it is not (and thus this descriptor is the end of the chain).
    fn next(&self) -> Option<u16> {
        if self.flags.contains(DescFlags::NEXT) {
            Some(self.next)
        } else {
            None
        }
    }
}

/// Descriptor flags
#[derive(
    Copy, Clone, Debug, Default, Eq, FromBytes, Immutable, IntoBytes, KnownLayout, PartialEq,
)]
#[repr(transparent)]
struct DescFlags(u16);

bitflags! {
    impl DescFlags: u16 {
        const NEXT = 1;
        const WRITE = 2;
        const INDIRECT = 4;
    }
}

/// The driver uses the available ring to offer buffers to the device:
/// each ring entry refers to the head of a descriptor chain.
/// It is only written by the driver and read by the device.
#[repr(C)]
#[derive(Debug)]
struct AvailRing<const SIZE: usize> {
    flags: AtomicU16,
    /// A driver MUST NOT decrement the idx.
    idx: AtomicU16,
    ring: [u16; SIZE],
    /// Only used if `VIRTIO_F_EVENT_IDX` is negotiated.
    used_event: AtomicU16,
}

/// The used ring is where the device returns buffers once it is done with them:
/// it is only written to by the device, and read by the driver.
#[repr(C)]
#[derive(Debug)]
struct UsedRing<const SIZE: usize> {
    flags: AtomicU16,
    idx: AtomicU16,
    ring: [UsedElem; SIZE],
    /// Only used if `VIRTIO_F_EVENT_IDX` is negotiated.
    avail_event: AtomicU16,
}

#[repr(C)]
#[derive(Debug)]
struct UsedElem {
    id: u32,
    len: u32,
}

struct InputOutputIter<'a, 'b> {
    inputs: &'a [&'b [u8]],
    outputs: &'a mut [&'b mut [u8]],
}

impl<'a, 'b> InputOutputIter<'a, 'b> {
    fn new(inputs: &'a [&'b [u8]], outputs: &'a mut [&'b mut [u8]]) -> Self {
        Self { inputs, outputs }
    }
}

impl Iterator for InputOutputIter<'_, '_> {
    type Item = (NonNull<[u8]>, BufferDirection);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(input) = take_first(&mut self.inputs) {
            Some(((*input).into(), BufferDirection::DriverToDevice))
        } else {
            let output = take_first_mut(&mut self.outputs)?;
            Some(((*output).into(), BufferDirection::DeviceToDriver))
        }
    }
}

// TODO: Use `slice::take_first` once it is stable
// (https://github.com/rust-lang/rust/issues/62280).
fn take_first<'a, T>(slice: &mut &'a [T]) -> Option<&'a T> {
    let (first, rem) = slice.split_first()?;
    *slice = rem;
    Some(first)
}

// TODO: Use `slice::take_first_mut` once it is stable
// (https://github.com/rust-lang/rust/issues/62280).
fn take_first_mut<'a, T>(slice: &mut &'a mut [T]) -> Option<&'a mut T> {
    let (first, rem) = take(slice).split_first_mut()?;
    *slice = rem;
    Some(first)
}

/// Simulates the device reading from a VirtIO queue and writing a response back, for use in tests.
///
/// The fake device always uses descriptors in order.
///
/// Returns true if a descriptor chain was available and processed, or false if no descriptors were
/// available.
#[cfg(test)]
pub(crate) fn fake_read_write_queue<const QUEUE_SIZE: usize>(
    descriptors: *const [Descriptor; QUEUE_SIZE],
    queue_driver_area: *const u8,
    queue_device_area: *mut u8,
    handler: impl FnOnce(Vec<u8>) -> Vec<u8>,
) -> bool {
    use core::{ops::Deref, slice};

    let available_ring = queue_driver_area as *const AvailRing<QUEUE_SIZE>;
    let used_ring = queue_device_area as *mut UsedRing<QUEUE_SIZE>;

    // Safe because the various pointers are properly aligned, dereferenceable, initialised, and
    // nothing else accesses them during this block.
    unsafe {
        // Make sure there is actually at least one descriptor available to read from.
        if (*available_ring).idx.load(Ordering::Acquire) == (*used_ring).idx.load(Ordering::Acquire)
        {
            return false;
        }
        // The fake device always uses descriptors in order, like VIRTIO_F_IN_ORDER, so
        // `used_ring.idx` marks the next descriptor we should take from the available ring.
        let next_slot = (*used_ring).idx.load(Ordering::Acquire) & (QUEUE_SIZE as u16 - 1);
        let head_descriptor_index = (*available_ring).ring[next_slot as usize];
        let mut descriptor = &(*descriptors)[head_descriptor_index as usize];

        let input_length;
        let output;
        if descriptor.flags.contains(DescFlags::INDIRECT) {
            // The descriptor shouldn't have any other flags if it is indirect.
            assert_eq!(descriptor.flags, DescFlags::INDIRECT);

            // Loop through all input descriptors in the indirect descriptor list, reading data from
            // them.
            let indirect_descriptor_list: &[Descriptor] = zerocopy::Ref::into_ref(
                zerocopy::Ref::<_, [Descriptor]>::from_bytes(slice::from_raw_parts(
                    descriptor.addr as *const u8,
                    descriptor.len as usize,
                ))
                .unwrap(),
            );
            let mut input = Vec::new();
            let mut indirect_descriptor_index = 0;
            while indirect_descriptor_index < indirect_descriptor_list.len() {
                let indirect_descriptor = &indirect_descriptor_list[indirect_descriptor_index];
                if indirect_descriptor.flags.contains(DescFlags::WRITE) {
                    break;
                }

                input.extend_from_slice(slice::from_raw_parts(
                    indirect_descriptor.addr as *const u8,
                    indirect_descriptor.len as usize,
                ));

                indirect_descriptor_index += 1;
            }
            input_length = input.len();

            // Let the test handle the request.
            output = handler(input);

            // Write the response to the remaining descriptors.
            let mut remaining_output = output.deref();
            while indirect_descriptor_index < indirect_descriptor_list.len() {
                let indirect_descriptor = &indirect_descriptor_list[indirect_descriptor_index];
                assert!(indirect_descriptor.flags.contains(DescFlags::WRITE));

                let length_to_write = min(remaining_output.len(), indirect_descriptor.len as usize);
                ptr::copy(
                    remaining_output.as_ptr(),
                    indirect_descriptor.addr as *mut u8,
                    length_to_write,
                );
                remaining_output = &remaining_output[length_to_write..];

                indirect_descriptor_index += 1;
            }
            assert_eq!(remaining_output.len(), 0);
        } else {
            // Loop through all input descriptors in the chain, reading data from them.
            let mut input = Vec::new();
            while !descriptor.flags.contains(DescFlags::WRITE) {
                input.extend_from_slice(slice::from_raw_parts(
                    descriptor.addr as *const u8,
                    descriptor.len as usize,
                ));

                if let Some(next) = descriptor.next() {
                    descriptor = &(*descriptors)[next as usize];
                } else {
                    break;
                }
            }
            input_length = input.len();

            // Let the test handle the request.
            output = handler(input);

            // Write the response to the remaining descriptors.
            let mut remaining_output = output.deref();
            if descriptor.flags.contains(DescFlags::WRITE) {
                loop {
                    assert!(descriptor.flags.contains(DescFlags::WRITE));

                    let length_to_write = min(remaining_output.len(), descriptor.len as usize);
                    ptr::copy(
                        remaining_output.as_ptr(),
                        descriptor.addr as *mut u8,
                        length_to_write,
                    );
                    remaining_output = &remaining_output[length_to_write..];

                    if let Some(next) = descriptor.next() {
                        descriptor = &(*descriptors)[next as usize];
                    } else {
                        break;
                    }
                }
            }
            assert_eq!(remaining_output.len(), 0);
        }

        // Mark the buffer as used.
        (*used_ring).ring[next_slot as usize].id = head_descriptor_index.into();
        (*used_ring).ring[next_slot as usize].len = (input_length + output.len()) as u32;
        (*used_ring).idx.fetch_add(1, Ordering::AcqRel);

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        device::common::Feature,
        hal::fake::FakeHal,
        transport::{
            fake::{FakeTransport, QueueStatus, State},
            mmio::{MmioTransport, VirtIOHeader, MODERN_VERSION},
            DeviceType,
        },
    };
    use core::array;
    use core::ptr::NonNull;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn queue_too_big() {
        let mut header = VirtIOHeader::make_fake_header(MODERN_VERSION, 1, 0, 0, 4);
        let mut transport =
            unsafe { MmioTransport::new(NonNull::from(&mut header), size_of::<VirtIOHeader>()) }
                .unwrap();
        assert_eq!(
            VirtQueue::<FakeHal, 8>::new(&mut transport, 0, false, false).unwrap_err(),
            Error::InvalidParam
        );
    }

    #[test]
    fn queue_already_used() {
        let mut header = VirtIOHeader::make_fake_header(MODERN_VERSION, 1, 0, 0, 4);
        let mut transport =
            unsafe { MmioTransport::new(NonNull::from(&mut header), size_of::<VirtIOHeader>()) }
                .unwrap();
        VirtQueue::<FakeHal, 4>::new(&mut transport, 0, false, false).unwrap();
        assert_eq!(
            VirtQueue::<FakeHal, 4>::new(&mut transport, 0, false, false).unwrap_err(),
            Error::AlreadyUsed
        );
    }

    #[test]
    fn add_empty() {
        let mut header = VirtIOHeader::make_fake_header(MODERN_VERSION, 1, 0, 0, 4);
        let mut transport =
            unsafe { MmioTransport::new(NonNull::from(&mut header), size_of::<VirtIOHeader>()) }
                .unwrap();
        let mut queue = VirtQueue::<FakeHal, 4>::new(&mut transport, 0, false, false).unwrap();
        assert_eq!(
            unsafe { queue.add(&[], &mut []) }.unwrap_err(),
            Error::InvalidParam
        );
    }

    #[test]
    fn add_too_many() {
        let mut header = VirtIOHeader::make_fake_header(MODERN_VERSION, 1, 0, 0, 4);
        let mut transport =
            unsafe { MmioTransport::new(NonNull::from(&mut header), size_of::<VirtIOHeader>()) }
                .unwrap();
        let mut queue = VirtQueue::<FakeHal, 4>::new(&mut transport, 0, false, false).unwrap();
        assert_eq!(queue.available_desc(), 4);
        assert_eq!(
            unsafe { queue.add(&[&[], &[], &[]], &mut [&mut [], &mut []]) }.unwrap_err(),
            Error::QueueFull
        );
    }

    #[test]
    fn add_buffers() {
        let mut header = VirtIOHeader::make_fake_header(MODERN_VERSION, 1, 0, 0, 4);
        let mut transport =
            unsafe { MmioTransport::new(NonNull::from(&mut header), size_of::<VirtIOHeader>()) }
                .unwrap();
        let mut queue = VirtQueue::<FakeHal, 4>::new(&mut transport, 0, false, false).unwrap();
        assert_eq!(queue.available_desc(), 4);

        // Add a buffer chain consisting of two device-readable parts followed by two
        // device-writable parts.
        let token = unsafe { queue.add(&[&[1, 2], &[3]], &mut [&mut [0, 0], &mut [0]]) }.unwrap();

        assert_eq!(queue.available_desc(), 0);
        assert!(!queue.can_pop());

        // Safe because the various parts of the queue are properly aligned, dereferenceable and
        // initialised, and nothing else is accessing them at the same time.
        unsafe {
            let first_descriptor_index = (*queue.avail.as_ptr()).ring[0];
            assert_eq!(first_descriptor_index, token);
            assert_eq!(
                (*queue.desc.as_ptr())[first_descriptor_index as usize].len,
                2
            );
            assert_eq!(
                (*queue.desc.as_ptr())[first_descriptor_index as usize].flags,
                DescFlags::NEXT
            );
            let second_descriptor_index =
                (*queue.desc.as_ptr())[first_descriptor_index as usize].next;
            assert_eq!(
                (*queue.desc.as_ptr())[second_descriptor_index as usize].len,
                1
            );
            assert_eq!(
                (*queue.desc.as_ptr())[second_descriptor_index as usize].flags,
                DescFlags::NEXT
            );
            let third_descriptor_index =
                (*queue.desc.as_ptr())[second_descriptor_index as usize].next;
            assert_eq!(
                (*queue.desc.as_ptr())[third_descriptor_index as usize].len,
                2
            );
            assert_eq!(
                (*queue.desc.as_ptr())[third_descriptor_index as usize].flags,
                DescFlags::NEXT | DescFlags::WRITE
            );
            let fourth_descriptor_index =
                (*queue.desc.as_ptr())[third_descriptor_index as usize].next;
            assert_eq!(
                (*queue.desc.as_ptr())[fourth_descriptor_index as usize].len,
                1
            );
            assert_eq!(
                (*queue.desc.as_ptr())[fourth_descriptor_index as usize].flags,
                DescFlags::WRITE
            );
        }
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn add_buffers_indirect() {
        use core::ptr::slice_from_raw_parts;

        let mut header = VirtIOHeader::make_fake_header(MODERN_VERSION, 1, 0, 0, 4);
        let mut transport =
            unsafe { MmioTransport::new(NonNull::from(&mut header), size_of::<VirtIOHeader>()) }
                .unwrap();
        let mut queue = VirtQueue::<FakeHal, 4>::new(&mut transport, 0, true, false).unwrap();
        assert_eq!(queue.available_desc(), 4);

        // Add a buffer chain consisting of two device-readable parts followed by two
        // device-writable parts.
        let token = unsafe { queue.add(&[&[1, 2], &[3]], &mut [&mut [0, 0], &mut [0]]) }.unwrap();

        assert_eq!(queue.available_desc(), 4);
        assert!(!queue.can_pop());

        // Safe because the various parts of the queue are properly aligned, dereferenceable and
        // initialised, and nothing else is accessing them at the same time.
        unsafe {
            let indirect_descriptor_index = (*queue.avail.as_ptr()).ring[0];
            assert_eq!(indirect_descriptor_index, token);
            assert_eq!(
                (*queue.desc.as_ptr())[indirect_descriptor_index as usize].len as usize,
                4 * size_of::<Descriptor>()
            );
            assert_eq!(
                (*queue.desc.as_ptr())[indirect_descriptor_index as usize].flags,
                DescFlags::INDIRECT
            );

            let indirect_descriptors = slice_from_raw_parts(
                (*queue.desc.as_ptr())[indirect_descriptor_index as usize].addr
                    as *const Descriptor,
                4,
            );
            assert_eq!((*indirect_descriptors)[0].len, 2);
            assert_eq!((*indirect_descriptors)[0].flags, DescFlags::NEXT);
            assert_eq!((*indirect_descriptors)[0].next, 1);
            assert_eq!((*indirect_descriptors)[1].len, 1);
            assert_eq!((*indirect_descriptors)[1].flags, DescFlags::NEXT);
            assert_eq!((*indirect_descriptors)[1].next, 2);
            assert_eq!((*indirect_descriptors)[2].len, 2);
            assert_eq!(
                (*indirect_descriptors)[2].flags,
                DescFlags::NEXT | DescFlags::WRITE
            );
            assert_eq!((*indirect_descriptors)[2].next, 3);
            assert_eq!((*indirect_descriptors)[3].len, 1);
            assert_eq!((*indirect_descriptors)[3].flags, DescFlags::WRITE);
        }
    }

    /// Tests that the queue advises the device that notifications are needed.
    #[test]
    fn set_dev_notify() {
        let state = Arc::new(Mutex::new(State::new(vec![QueueStatus::default()], ())));
        let mut transport = FakeTransport {
            device_type: DeviceType::Block,
            max_queue_size: 4,
            device_features: 0,
            state: state.clone(),
        };
        let mut queue = VirtQueue::<FakeHal, 4>::new(&mut transport, 0, false, false).unwrap();

        // Check that the avail ring's flag is zero by default.
        assert_eq!(
            unsafe { (*queue.avail.as_ptr()).flags.load(Ordering::Acquire) },
            0x0
        );

        queue.set_dev_notify(false);

        // Check that the avail ring's flag is 1 after `disable_dev_notify`.
        assert_eq!(
            unsafe { (*queue.avail.as_ptr()).flags.load(Ordering::Acquire) },
            0x1
        );

        queue.set_dev_notify(true);

        // Check that the avail ring's flag is 0 after `enable_dev_notify`.
        assert_eq!(
            unsafe { (*queue.avail.as_ptr()).flags.load(Ordering::Acquire) },
            0x0
        );
    }

    /// Tests that the queue notifies the device about added buffers, if it hasn't suppressed
    /// notifications.
    #[test]
    fn add_notify() {
        let state = Arc::new(Mutex::new(State::new(vec![QueueStatus::default()], ())));
        let mut transport = FakeTransport {
            device_type: DeviceType::Block,
            max_queue_size: 4,
            device_features: 0,
            state: state.clone(),
        };
        let mut queue = VirtQueue::<FakeHal, 4>::new(&mut transport, 0, false, false).unwrap();

        // Add a buffer chain with a single device-readable part.
        unsafe { queue.add(&[&[42]], &mut []) }.unwrap();

        // Check that the transport would be notified.
        assert_eq!(queue.should_notify(), true);

        // SAFETY: the various parts of the queue are properly aligned, dereferenceable and
        // initialised, and nothing else is accessing them at the same time.
        unsafe {
            // Suppress notifications.
            (*queue.used.as_ptr()).flags.store(0x01, Ordering::Release);
        }

        // Check that the transport would not be notified.
        assert_eq!(queue.should_notify(), false);
    }

    /// Tests that the queue notifies the device about added buffers, if it hasn't suppressed
    /// notifications with the `avail_event` index.
    #[test]
    fn add_notify_event_idx() {
        let state = Arc::new(Mutex::new(State::new(vec![QueueStatus::default()], ())));
        let mut transport = FakeTransport {
            device_type: DeviceType::Block,
            max_queue_size: 4,
            device_features: Feature::RING_EVENT_IDX.bits(),
            state: state.clone(),
        };
        let mut queue = VirtQueue::<FakeHal, 4>::new(&mut transport, 0, false, true).unwrap();

        // Add a buffer chain with a single device-readable part.
        assert_eq!(unsafe { queue.add(&[&[42]], &mut []) }.unwrap(), 0);

        // Check that the transport would be notified.
        assert_eq!(queue.should_notify(), true);

        // SAFETY: the various parts of the queue are properly aligned, dereferenceable and
        // initialised, and nothing else is accessing them at the same time.
        unsafe {
            // Suppress notifications.
            (*queue.used.as_ptr())
                .avail_event
                .store(1, Ordering::Release);
        }

        // Check that the transport would not be notified.
        assert_eq!(queue.should_notify(), false);

        // Add another buffer chain.
        assert_eq!(unsafe { queue.add(&[&[42]], &mut []) }.unwrap(), 1);

        // Check that the transport should be notified again now.
        assert_eq!(queue.should_notify(), true);
    }

    struct VirtQueuePair<const SIZE: usize> {
        driver: VirtQueue<FakeHal, SIZE>,
        device: DeviceVirtQueue<FakeHal, SIZE>,
        transport: FakeTransport<()>,
    }

    // Create a device/driver virtqueue pair which share memory in the test process's virtual
    // address space
    fn create_queues<const SIZE: usize>(device_type: DeviceType) -> VirtQueuePair<SIZE> {
        let mut header = VirtIOHeader::make_fake_header(MODERN_VERSION, 1, 0, 0, 4);
        let state = Arc::new(Mutex::new(State::new(vec![QueueStatus::default()], ())));
        let mut transport = FakeTransport {
            device_type,
            max_queue_size: SIZE as u32,
            device_features: 0,
            state: state.clone(),
        };
        let driver = VirtQueue::<FakeHal, SIZE>::new(&mut transport, 0, false, true).unwrap();
        let device = DeviceVirtQueue::<FakeHal, SIZE>::new(&mut transport, 0).unwrap();
        VirtQueuePair {
            driver,
            device,
            transport,
        }
    }

    // Run a test with the given callbacks using a virtqueue pair. Since this spins up new threads
    // we must assert whether the threads join or not to ensure that asserts in the callback get
    // called before the test's main thread returns.
    fn queue_pair_test<const SIZE: usize>(
        driver_func: impl FnOnce(VirtQueue<FakeHal, SIZE>, FakeTransport<()>) + Send + 'static,
        device_func: impl FnOnce(DeviceVirtQueue<FakeHal, SIZE>, FakeTransport<()>) + Send + 'static,
    ) {
        let mut queues = create_queues::<SIZE>(DeviceType::Socket);
        let mut dev_transport = queues.transport.clone();
        let driver_handle = thread::spawn(move || driver_func(queues.driver, queues.transport));
        let device_handle = thread::spawn(move || device_func(queues.device, dev_transport));
        // If the driver panics while the device is waiting on it this is expected to hang.
        assert!(device_handle.join().is_ok());
        assert!(driver_handle.join().is_ok());
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn simple_send_to_device() {
        // This test sends [0..10] using 1 10-byte descriptor
        let mut data: [u8; 10] = array::from_fn(|i| i as u8);
        queue_pair_test::<8>(
            move |mut driver, mut transport| {
                driver
                    .add_notify_wait_pop(&[&data], &mut [], &mut transport)
                    .unwrap();
            },
            move |mut device, mut transport| {
                // Wait until the driver adds to the avail vring
                while !device.can_pop() {
                    spin_loop();
                }
                let poll_res = device
                    .poll(&mut transport, |buffer| {
                        // Make sure what's read from the buffers matches what was send in
                        // add_notify_wait_pop
                        assert_eq!(buffer, data);
                        Ok(Some(()))
                    })
                    .unwrap();
                // Make sure that polling actually invoked the callback
                assert!(poll_res.is_some());
            },
        );
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn split_send_to_device() {
        // This test sends [0..10] using 10 1-byte descriptors
        // Data sent from the device using multiple descriptors
        let driver_data: [[u8; 1]; 10] = array::from_fn(|i| [i as u8]);
        // Data in a single descriptor as the device is expected to receive it
        let device_data: [u8; 10] = array::from_fn(|i| i as u8);

        queue_pair_test::<16>(
            move |mut driver, mut transport| {
                // Creates a &[&[u8]] from driver_data and sends it to the device
                driver
                    .add_notify_wait_pop(
                        array::from_fn::<&[u8], 10, _>(|i| driver_data[i].as_slice()).as_slice(),
                        &mut [],
                        &mut transport,
                    )
                    .unwrap();
            },
            move |mut device, mut transport| {
                // Wait until the driver adds to the avail vring
                while !device.can_pop() {
                    spin_loop();
                }
                let poll_res = device
                    .poll(&mut transport, |buffer| {
                        assert_eq!(buffer, device_data);
                        Ok(Some(()))
                    })
                    .unwrap();
                // Make sure that polling actually invoked the callback
                assert!(poll_res.is_some());
            },
        );
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn recv_from_device() {
        // This test makes 1 10-byte descriptor available to the device and receives [0..10] in the
        // driver using it.
        // A buffer for the driver to receive the data in
        let mut buffer = [0u8; 10];
        // The data the device will send
        let data: [u8; 10] = array::from_fn(|i| i as u8);
        queue_pair_test::<8>(
            move |mut driver, mut transport| {
                assert_eq!(buffer, [0; 10]);
                // Add a write descriptor for the device to use then pop it
                driver
                    .add_notify_wait_pop(&[], &mut [&mut buffer], &mut transport)
                    .unwrap();
                // Make sure the device wrote the expected data to the buffer
                assert_eq!(buffer, data);
            },
            move |mut device, mut transport| {
                // Wait until the driver adds a descriptor and write the contents of data to it
                device
                    .wait_pop_add_notify(&[&data], &mut transport)
                    .unwrap();
            },
        );
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn recv_from_device_with_retry() {
        // In this test the driver makes a read descriptor available, the device pops it to try to
        // send some data but returns Err. The driver then makes a write descriptor available and
        // the device retries and succeeds at sending the data.
        let mut buffer = [0u8; 10];
        let data: [u8; 10] = array::from_fn(|i| i as u8);
        queue_pair_test::<8>(
            move |mut driver, mut transport| {
                // Add a 1-byte read descriptor to the avail vring
                let read_buffer = [0; 1];
                unsafe {
                    driver.add(&[&read_buffer], &mut []).unwrap();
                }
                // Make sure the device didn't try to write to the buffer
                assert_eq!(buffer, [0; 10]);
                // Add 1 10-byte write descriptor to the avail vring
                driver
                    .add_notify_wait_pop(&[], &mut [&mut buffer], &mut transport)
                    .unwrap();
                // Make sure the device wrote to the second descriptor
                assert_eq!(buffer, data);
            },
            move |mut device, mut transport| {
                // Wait until there's a descriptor in the avail vring
                let res = device.wait_pop_add_notify(&[&data], &mut transport);
                // The first descriptor will be read-only so wait_pop_add_notify should return Err
                assert_eq!(res, Err(Error::NotReady));
                // Wait until there's another descriptor added and use that to send data
                device
                    .wait_pop_add_notify(&[&data], &mut transport)
                    .unwrap();
            },
        );
    }
}
