//! Virtual Filesystem for Eryx Sandbox
//!
//! Provides a custom `wasi:filesystem` implementation backed by a key-value
//! store, allowing sandboxed Python code to read and write files that persist
//! across sandbox executions.
//!
//! ## Architecture
//!
//! The VFS consists of:
//! - [`VfsStorage`] - A trait for pluggable storage backends
//! - [`InMemoryStorage`] - An in-memory implementation for testing
//! - WASI host implementations that bridge storage to the component model
//!
//! ## Usage
//!
//! ```rust,ignore
//! use eryx_vfs::{InMemoryStorage, VfsCtx, VfsState, VfsView, add_vfs_to_linker};
//! use std::sync::Arc;
//!
//! // Create storage
//! let storage = Arc::new(InMemoryStorage::new());
//!
//! // Create VFS context
//! let mut vfs_ctx = VfsCtx::new(storage);
//!
//! // Add WASI to linker first, then override filesystem with VFS
//! wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
//! add_vfs_to_linker(&mut linker)?;
//! ```

#![deny(unsafe_code)]

include!(concat!(env!("OUT_DIR"), "/binding_modules.rs"));
mod error;
mod file_io;
mod host;
pub mod hybrid;
mod hybrid_host;
mod linker;
pub mod scrubbing;
mod storage;
mod streams;
mod wasi_impl;

pub use error::{VfsError, VfsResult};
pub use hybrid::{
    HybridDescriptor, HybridPreopen, HybridVfsCtx, HybridVfsState, RealDir, RealFile, RestrictedDir,
};
pub use hybrid_bindings::HybridReaddirIterator;
pub use linker::{
    HybridVfsView, VfsView, add_hybrid_vfs_to_linker, add_vfs_to_linker,
    hybrid_filesystem_wasi_version,
};
pub use scrubbing::{
    FileScrubPolicy as VfsFileScrubPolicy, ScrubbingStorage, SecretConfig as VfsSecretConfig,
};
pub use storage::{ArcStorage, DirEntry, InMemoryStorage, Metadata, VfsStorage};
pub use wasi_impl::{VfsCtx, VfsDescriptor, VfsReaddirIterator, VfsState};

// Keep separate VFS directory and file permissions after Wasmtime 48 replaced
// its permission flags with FsPerms.
bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct DirPerms: usize {
        const READ = 0b1;
        const MUTATE = 0b10;
    }

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct FilePerms: usize {
        const READ = 0b1;
        const WRITE = 0b10;
    }
}
