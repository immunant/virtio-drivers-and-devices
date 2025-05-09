#[cfg(test)]
pub mod fake;

use crate::{Error, Result, PAGE_SIZE};
use core::cmp::PartialEq;
use core::{marker::PhantomData, ptr::NonNull};

/// A physical address as used for virtio.
pub type PhysAddr = usize;

pub trait DmaMemory {
    fn paddr(&self) -> usize;
    fn vaddr(&self, offset: usize) -> NonNull<u8>;
    fn raw_slice(&self) -> NonNull<[u8]>;
}

/// A region of contiguous physical memory used for DMA.
#[derive(Debug)]
pub struct Dma<H: Hal> {
    paddr: usize,
    vaddr: NonNull<u8>,
    pages: usize,
    _hal: PhantomData<H>,
}

// SAFETY: DMA memory can be accessed from any thread.
unsafe impl<H: Hal> Send for Dma<H> {}

// SAFETY: `&Dma` only allows pointers and physical addresses to be returned. Any actual access to
// the memory requires unsafe code, which is responsible for avoiding data races.
unsafe impl<H: Hal> Sync for Dma<H> {}

impl<H: Hal> Dma<H> {
    /// Allocates the given number of pages of physically contiguous memory to be used for DMA in
    /// the given direction.
    ///
    /// The pages will be zeroed.
    pub fn new(pages: usize, direction: BufferDirection) -> Result<Self> {
        let (paddr, vaddr) = H::dma_alloc(pages, direction);
        if paddr == 0 {
            return Err(Error::DmaError);
        }
        Ok(Self {
            paddr,
            vaddr,
            pages,
            _hal: PhantomData,
        })
    }
}

impl<H: Hal> DmaMemory for Dma<H> {
    /// Returns the physical address of the start of the DMA region, as seen by devices.
    fn paddr(&self) -> usize {
        self.paddr
    }

    /// Returns a pointer to the given offset within the DMA region.
    fn vaddr(&self, offset: usize) -> NonNull<u8> {
        assert!(offset < self.pages * PAGE_SIZE);
        NonNull::new((self.vaddr.as_ptr() as usize + offset) as _).unwrap()
    }

    /// Returns a pointer to the entire DMA region as a slice.
    fn raw_slice(&self) -> NonNull<[u8]> {
        let raw_slice =
            core::ptr::slice_from_raw_parts_mut(self.vaddr(0).as_ptr(), self.pages * PAGE_SIZE);
        NonNull::new(raw_slice).unwrap()
    }
}

impl<H: Hal> Drop for Dma<H> {
    fn drop(&mut self) {
        // SAFETY: The memory was previously allocated by `dma_alloc` in `Dma::new`,
        // not yet deallocated, and we are passing the values from then.
        let err = unsafe { H::dma_dealloc(self.paddr, self.vaddr, self.pages) };
        assert_eq!(err, 0, "failed to deallocate DMA");
    }
}

#[derive(Debug)]
pub struct DeviceDma<H: DeviceHal> {
    paddr: usize,
    vaddr: NonNull<u8>,
    pages: usize,
    _hal: PhantomData<H>,
    client_id: u16,
}

// SAFETY: Device DMA memory can be accessed from any thread.
unsafe impl<H: DeviceHal> Send for DeviceDma<H> {}

// SAFETY: `&DeviceDma` only allows pointers and physical addresses to be returned. Any accesses to
// the memory requires unsafe code, which is responsible for avoiding data races.
unsafe impl<H: DeviceHal> Sync for DeviceDma<H> {}

impl<H: DeviceHal> PartialEq for DeviceDma<H> {
    fn eq(&self, other: &Self) -> bool {
        let paddrs_match = self.paddr == other.paddr;
        let vaddrs_match = self.vaddr == other.vaddr;
        let num_pages_match = self.pages == other.pages;
        paddrs_match && vaddrs_match && num_pages_match
    }
}

impl<H: DeviceHal> DeviceDma<H> {
    // SAFETY: The caller must ensure that the memory described by paddr and pages can be mapped by
    // the type implementing DeviceHal such as a virtqueue or a buffer described by a descriptor.
    pub unsafe fn new(
        paddr: PhysAddr,
        pages: usize,
        direction: BufferDirection,
        client_id: u16,
    ) -> Result<Self> {
        let vaddr = H::dma_map(paddr, pages, direction, client_id)?;
        Ok(Self {
            paddr,
            vaddr,
            pages,
            _hal: PhantomData,
            client_id,
        })
    }
}

impl<H: DeviceHal> DmaMemory for DeviceDma<H> {
    /// Returns the physical address of the start of the DMA region, as seen by devices.
    fn paddr(&self) -> usize {
        self.paddr
    }

    /// Returns a pointer to the given offset within the DMA region.
    fn vaddr(&self, offset: usize) -> NonNull<u8> {
        assert!(offset < self.pages * PAGE_SIZE);
        NonNull::new((self.vaddr.as_ptr() as usize + offset) as _).unwrap()
    }

    /// Returns a pointer to the entire DMA region as a slice.
    fn raw_slice(&self) -> NonNull<[u8]> {
        let raw_slice =
            core::ptr::slice_from_raw_parts_mut(self.vaddr(0).as_ptr(), self.pages * PAGE_SIZE);
        NonNull::new(raw_slice).unwrap()
    }
}

impl<H: DeviceHal> Drop for DeviceDma<H> {
    fn drop(&mut self) {
        // SAFETY: DeviceDma::new ensures that paddr, vaddr and pages were passed to
        // DeviceHal::dma_map for this instance of DeviceDma
        let err = unsafe { H::dma_unmap(self.paddr, self.vaddr, self.pages) };
        assert_eq!(err, 0, "failed to unmap DMA");
    }
}

/// The interface which a particular hardware implementation must implement.
///
/// # Safety
///
/// Implementations of this trait must follow the "implementation safety" requirements documented
/// for each method. Callers must follow the safety requirements documented for the unsafe methods.
pub unsafe trait Hal {
    /// Allocates and zeroes the given number of contiguous physical pages of DMA memory for VirtIO
    /// use.
    ///
    /// Returns both the physical address which the device can use to access the memory, and a
    /// pointer to the start of it which the driver can use to access it.
    ///
    /// # Implementation safety
    ///
    /// Implementations of this method must ensure that the `NonNull<u8>` returned is a
    /// [_valid_](https://doc.rust-lang.org/std/ptr/index.html#safety) pointer, aligned to
    /// [`PAGE_SIZE`], and won't alias any other allocations or references in the program until it
    /// is deallocated by `dma_dealloc`. The pages must be zeroed.
    fn dma_alloc(pages: usize, direction: BufferDirection) -> (PhysAddr, NonNull<u8>);

    /// Deallocates the given contiguous physical DMA memory pages.
    ///
    /// # Safety
    ///
    /// The memory must have been allocated by `dma_alloc` on the same `Hal` implementation, and not
    /// yet deallocated. `pages` must be the same number passed to `dma_alloc` originally, and both
    /// `paddr` and `vaddr` must be the values returned by `dma_alloc`.
    unsafe fn dma_dealloc(paddr: PhysAddr, vaddr: NonNull<u8>, pages: usize) -> i32;

    /// Converts a physical address used for MMIO to a virtual address which the driver can access.
    ///
    /// This is only used for MMIO addresses within BARs read from the device, for the PCI
    /// transport. It may check that the address range up to the given size is within the region
    /// expected for MMIO.
    ///
    /// # Implementation safety
    ///
    /// Implementations of this method must ensure that the `NonNull<u8>` returned is a
    /// [_valid_](https://doc.rust-lang.org/std/ptr/index.html#safety) pointer, and won't alias any
    /// other allocations or references in the program.
    ///
    /// # Safety
    ///
    /// The `paddr` and `size` must describe a valid MMIO region. The implementation may validate it
    /// in some way (and panic if it is invalid) but is not guaranteed to.
    unsafe fn mmio_phys_to_virt(paddr: PhysAddr, size: usize) -> NonNull<u8>;

    /// Shares the given memory range with the device, and returns the physical address that the
    /// device can use to access it.
    ///
    /// This may involve mapping the buffer into an IOMMU, giving the host permission to access the
    /// memory, or copying it to a special region where it can be accessed.
    ///
    /// # Safety
    ///
    /// The buffer must be a valid pointer to a non-empty memory range which will not be accessed by
    /// any other thread for the duration of this method call.
    unsafe fn share(buffer: NonNull<[u8]>, direction: BufferDirection) -> PhysAddr;

    /// Unshares the given memory range from the device and (if necessary) copies it back to the
    /// original buffer.
    ///
    /// # Safety
    ///
    /// The buffer must be a valid pointer to a non-empty memory range which will not be accessed by
    /// any other thread for the duration of this method call. The `paddr` must be the value
    /// previously returned by the corresponding `share` call.
    unsafe fn unshare(paddr: PhysAddr, buffer: NonNull<[u8]>, direction: BufferDirection);
}

/// Device-side abstraction layer for mapping and unmapping memory shared by the driver.
///
/// # Safety
///
/// Implementations of this trait must follow the "implementation safety" requirements documented
/// for each method. Callers must follow the safety requirements documented for the unsafe methods.
pub trait DeviceHal {
    /// Maps in memory for a range of physical addresses shared by a VirtIO driver.
    ///
    /// Returns the virtual address which the device should use to access it.
    /// # Implementation safety
    ///
    /// Implementations of this method must ensure that the `NonNull<u8>` returned is a
    /// [_valid_](https://doc.rust-lang.org/std/ptr/index.html#safety) pointer, aligned to
    /// [`PAGE_SIZE`], and won't alias any other allocations or references in the program until it
    /// is freed by `dma_unmap`.
    unsafe fn dma_map(
        paddr: PhysAddr,
        pages: usize,
        direction: BufferDirection,
        client_id: u16,
    ) -> Result<NonNull<u8>>;

    /// Unmaps memory previously shared by the driver.
    ///
    /// # Safety
    ///
    /// The memory must have been mapped in by `dma_map` on the same `DeviceHal` implementation, and
    /// not yet unmapped. `pages` must be the same number passed to `dma_map` originally, and
    /// both `paddr` and `vaddr` must be the values returned by `dma_map`.
    unsafe fn dma_unmap(paddr: PhysAddr, vaddr: NonNull<u8>, pages: usize) -> i32;
}

/// The direction in which a buffer is passed.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BufferDirection {
    /// The buffer may be read or written by the driver, but only read by the device.
    DriverToDevice,
    /// The buffer may be read or written by the device, but only read by the driver.
    DeviceToDriver,
    /// The buffer may be read or written by both the device and the driver.
    Both,
}
