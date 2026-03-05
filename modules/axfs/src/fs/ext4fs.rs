use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec;
use core::cell::UnsafeCell;

use axfs_vfs::{VfsDirEntry, VfsError, VfsNodePerm, VfsResult};
use axfs_vfs::{VfsNodeAttr, VfsNodeOps, VfsNodeRef, VfsNodeType, VfsOps};
use axsync::Mutex;
use ext4_rs::{BlockDevice, Errno, Ext4, InodeFileType};

use crate::dev::Disk;

const BLOCK_SIZE: usize = 4096;
const ROOT_INODE: u32 = 2;

struct Ext4Disk {
    disk: Mutex<Disk>,
}

impl Ext4Disk {
    fn new(disk: Disk) -> Self {
        Self {
            disk: Mutex::new(disk),
        }
    }
}

impl BlockDevice for Ext4Disk {
    fn read_offset(&self, offset: usize) -> alloc::vec::Vec<u8> {
        let mut out = vec![0u8; BLOCK_SIZE];
        let mut disk = self.disk.lock();
        disk.set_position(offset as u64);

        let mut done = 0;
        while done < out.len() {
            let n = disk
                .read_one(&mut out[done..])
                .expect("ext4 disk read failed");
            if n == 0 {
                break;
            }
            done += n;
        }
        out
    }

    fn write_offset(&self, offset: usize, data: &[u8]) {
        let mut disk = self.disk.lock();
        disk.set_position(offset as u64);

        let mut done = 0;
        while done < data.len() {
            let n = disk.write_one(&data[done..]).expect("ext4 disk write failed");
            if n == 0 {
                break;
            }
            done += n;
        }
    }
}

pub struct Ext4FileSystem {
    inner: Mutex<Ext4>,
    root_dir: UnsafeCell<Option<VfsNodeRef>>,
}

pub struct FileWrapper {
    fs: &'static Ext4FileSystem,
    path: String,
}

pub struct DirWrapper {
    fs: &'static Ext4FileSystem,
    path: String,
}

unsafe impl Sync for Ext4FileSystem {}
unsafe impl Send for Ext4FileSystem {}
unsafe impl Send for FileWrapper {}
unsafe impl Sync for FileWrapper {}
unsafe impl Send for DirWrapper {}
unsafe impl Sync for DirWrapper {}

impl Ext4FileSystem {
    pub fn new(disk: Disk) -> Self {
        let dev: Arc<dyn BlockDevice> = Arc::new(Ext4Disk::new(disk));
        let inner = Ext4::open(dev);
        Self {
            inner: Mutex::new(inner),
            root_dir: UnsafeCell::new(None),
        }
    }

    pub fn init(&'static self) {
        unsafe {
            *self.root_dir.get() = Some(Arc::new(DirWrapper {
                fs: self,
                path: "/".into(),
            }));
        }
    }

    fn new_file(fs: &'static Self, path: String) -> Arc<FileWrapper> {
        Arc::new(FileWrapper { fs, path })
    }

    fn new_dir(fs: &'static Self, path: String) -> Arc<DirWrapper> {
        Arc::new(DirWrapper { fs, path })
    }

    fn join_path(base: &str, path: &str) -> String {
        let p = path.trim_matches('/');
        if p.is_empty() {
            return String::from(base);
        }
        if base == "/" {
            alloc::format!("/{}", p)
        } else {
            alloc::format!("{}/{}", base, p)
        }
    }

    fn split_parent_name(path: &str) -> Option<(String, String)> {
        let p = path.trim_matches('/');
        if p.is_empty() {
            return None;
        }
        if let Some(pos) = p.rfind('/') {
            if pos == 0 {
                Some(("/".into(), p[1..].into()))
            } else {
                Some((alloc::format!("/{}", &p[..pos]), p[pos + 1..].into()))
            }
        } else {
            Some(("/".into(), p.into()))
        }
    }

    fn lookup_inode(ext4: &Ext4, path: &str) -> VfsResult<u32> {
        if path == "/" {
            return Ok(ROOT_INODE);
        }
        let mut parent = ROOT_INODE;
        let mut name_off = 0;
        ext4
            .generic_open(path, &mut parent, false, InodeFileType::S_IFREG.bits(), &mut name_off)
            .map_err(ext4_err_to_vfs)
    }
}

impl VfsOps for Ext4FileSystem {
    fn root_dir(&self) -> VfsNodeRef {
        let root_dir = unsafe { (*self.root_dir.get()).as_ref().unwrap() };
        root_dir.clone()
    }

    fn umount(&self) -> VfsResult {
        Ok(())
    }
}

impl VfsNodeOps for FileWrapper {
    axfs_vfs::impl_vfs_non_dir_default! {}

    fn get_attr(&self) -> VfsResult<VfsNodeAttr> {
        let ext4 = self.fs.inner.lock();
        let inode = Ext4FileSystem::lookup_inode(&ext4, &self.path)?;
        let inode_ref = ext4.get_inode_ref(inode);
        let size = inode_ref.inode.size();
        let blocks = size.div_ceil(BLOCK_SIZE as u64);
        let perm = VfsNodePerm::from_bits_truncate(0o755);
        Ok(VfsNodeAttr::new(perm, VfsNodeType::File, size, blocks))
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> VfsResult<usize> {
        let ext4 = self.fs.inner.lock();
        let inode = Ext4FileSystem::lookup_inode(&ext4, &self.path)?;
        ext4.read_at(inode, offset as usize, buf).map_err(ext4_err_to_vfs)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> VfsResult<usize> {
        let ext4 = self.fs.inner.lock();
        let inode = Ext4FileSystem::lookup_inode(&ext4, &self.path)?;
        ext4.write_at(inode, offset as usize, buf).map_err(ext4_err_to_vfs)
    }

    fn truncate(&self, size: u64) -> VfsResult {
        let ext4 = self.fs.inner.lock();
        let inode = Ext4FileSystem::lookup_inode(&ext4, &self.path)?;
        let mut inode_ref = ext4.get_inode_ref(inode);
        let old_size = inode_ref.inode.size();

        if size == old_size {
            return Ok(());
        }

        if size < old_size {
            return ext4
                .truncate_inode(&mut inode_ref, size)
                .map(|_| ())
                .map_err(ext4_err_to_vfs);
        }

        // ext4_rs only exposes shrink truncate; extend by writing zeros.
        let zeros = [0u8; BLOCK_SIZE];
        let mut left = size - old_size;
        let mut off = old_size as usize;
        while left > 0 {
            let n = core::cmp::min(left as usize, zeros.len());
            ext4.write_at(inode, off, &zeros[..n]).map_err(ext4_err_to_vfs)?;
            off += n;
            left -= n as u64;
        }
        Ok(())
    }

    fn fsync(&self) -> VfsResult {
        Ok(())
    }
}

impl VfsNodeOps for DirWrapper {
    axfs_vfs::impl_vfs_dir_default! {}

    fn get_attr(&self) -> VfsResult<VfsNodeAttr> {
        let ext4 = self.fs.inner.lock();
        let inode = Ext4FileSystem::lookup_inode(&ext4, &self.path)?;
        let inode_ref = ext4.get_inode_ref(inode);
        let size = inode_ref.inode.size();
        let blocks = size.div_ceil(BLOCK_SIZE as u64);
        Ok(VfsNodeAttr::new(
            VfsNodePerm::from_bits_truncate(0o755),
            VfsNodeType::Dir,
            size,
            blocks,
        ))
    }

    fn parent(&self) -> Option<VfsNodeRef> {
        if self.path == "/" {
            return None;
        }

        let parent_path = if let Some(pos) = self.path.rfind('/') {
            if pos == 0 {
                "/".into()
            } else {
                self.path[..pos].into()
            }
        } else {
            "/".into()
        };

        Some(Ext4FileSystem::new_dir(self.fs, parent_path))
    }

    fn lookup(self: Arc<Self>, path: &str) -> VfsResult<VfsNodeRef> {
        debug!("lookup at ext4fs: {}", path);
        let path = path.trim_matches('/');

        if path.is_empty() || path == "." {
            return Ok(self.clone());
        }
        if path == ".." {
            return self.parent().ok_or(VfsError::NotFound);
        }
        if let Some(rest) = path.strip_prefix("./") {
            return self.lookup(rest);
        }

        let full_path = Ext4FileSystem::join_path(&self.path, path);
        let ext4 = self.fs.inner.lock();
        let inode = Ext4FileSystem::lookup_inode(&ext4, &full_path)?;
        let inode_ref = ext4.get_inode_ref(inode);

        if inode_ref.inode.is_dir() {
            Ok(Ext4FileSystem::new_dir(self.fs, full_path))
        } else {
            Ok(Ext4FileSystem::new_file(self.fs, full_path))
        }
    }

    fn create(&self, path: &str, ty: VfsNodeType) -> VfsResult {
        let full_path = Ext4FileSystem::join_path(&self.path, path);
        let (parent_path, name) = Ext4FileSystem::split_parent_name(&full_path)
            .ok_or(VfsError::InvalidInput)?;

        let ext4 = self.fs.inner.lock();

        if Ext4FileSystem::lookup_inode(&ext4, &full_path).is_ok() {
            return Err(VfsError::AlreadyExists);
        }

        let parent_inode = Ext4FileSystem::lookup_inode(&ext4, &parent_path)?;
        let mode = match ty {
            VfsNodeType::File => InodeFileType::S_IFREG.bits(),
            VfsNodeType::Dir => InodeFileType::S_IFDIR.bits(),
            _ => return Err(VfsError::Unsupported),
        };

        ext4.create(parent_inode, &name, mode)
            .map(|_| ())
            .map_err(ext4_err_to_vfs)
    }

    fn remove(&self, path: &str) -> VfsResult {
        let full_path = Ext4FileSystem::join_path(&self.path, path);
        let ext4 = self.fs.inner.lock();
        let inode = Ext4FileSystem::lookup_inode(&ext4, &full_path)?;
        let inode_ref = ext4.get_inode_ref(inode);

        if inode_ref.inode.is_dir() {
            let (parent_path, name) = Ext4FileSystem::split_parent_name(&full_path)
                .ok_or(VfsError::InvalidInput)?;
            let parent_inode = Ext4FileSystem::lookup_inode(&ext4, &parent_path)?;
            ext4.dir_remove(parent_inode, &name)
                .map(|_| ())
                .map_err(ext4_err_to_vfs)
        } else {
            ext4.file_remove(&full_path)
                .map(|_| ())
                .map_err(ext4_err_to_vfs)
        }
    }

    fn read_dir(&self, start_idx: usize, dirents: &mut [VfsDirEntry]) -> VfsResult<usize> {
        let ext4 = self.fs.inner.lock();
        let inode = Ext4FileSystem::lookup_inode(&ext4, &self.path)?;
        let entries = ext4.dir_get_entries(inode);

        let mut written = 0;
        for entry in entries.iter().skip(start_idx) {
            if written >= dirents.len() {
                break;
            }
            if entry.unused() {
                continue;
            }
            let name = entry.get_name();
            let ty = if ext4.get_inode_ref(entry.inode).inode.is_dir() {
                VfsNodeType::Dir
            } else {
                VfsNodeType::File
            };
            dirents[written] = VfsDirEntry::new(&name, ty);
            written += 1;
        }

        Ok(written)
    }

    fn rename(&self, _src_path: &str, _dst_path: &str) -> VfsResult {
        // ext4_rs 1.3.3 does not expose a public rename API in simple/fuse interfaces.
        Err(VfsError::Unsupported)
    }
}

fn ext4_err_to_vfs(err: ext4_rs::Ext4Error) -> VfsError {
    match err.error() {
        Errno::ENOENT => VfsError::NotFound,
        Errno::EEXIST => VfsError::AlreadyExists,
        Errno::ENOTDIR => VfsError::NotADirectory,
        Errno::EISDIR => VfsError::IsADirectory,
        Errno::ENOTSUP => VfsError::Unsupported,
        Errno::ENOSPC => VfsError::StorageFull,
        Errno::EINVAL => VfsError::InvalidInput,
        _ => VfsError::Io,
    }
}
