use alloc::{borrow::ToOwned, string::String, sync::Arc, vec};
use core::{any::Any, task::Context};

use axfs_ng_vfs::{
    DirEntry, DirEntrySink, DirNode, DirNodeOps, FileNode, FileNodeOps, FilesystemOps, Metadata,
    MetadataUpdate, NodeFlags, NodeOps, NodePermission, NodeType, Reference, VfsError, VfsResult,
    WeakDirEntry,
};
use axpoll::{IoEvents, Pollable};
use ext4_rs::{Errno, Ext4, Ext4DirEntry, Ext4DirSearchResult, Ext4InodeRef, FileAttr};

use super::{
    Ext4Filesystem,
    util::{
        duration_to_ext4_time, into_ext4_type, into_vfs_err, into_vfs_type, metadata_from_attr,
        now_as_ext4_time,
    },
};

pub struct Inode {
    fs: Arc<Ext4Filesystem>,
    ino: u32,
    this: Option<WeakDirEntry>,
}

impl Inode {
    pub(crate) fn new(fs: Arc<Ext4Filesystem>, ino: u32, this: Option<WeakDirEntry>) -> Arc<Self> {
        Arc::new(Self { fs, ino, this })
    }

    fn create_entry(&self, inode_ref: &Ext4InodeRef, name: impl Into<String>) -> DirEntry {
        let reference = Reference::new(
            self.this.as_ref().and_then(WeakDirEntry::upgrade),
            name.into(),
        );
        let node_type = into_vfs_type(inode_ref.inode.file_type());
        if inode_ref.inode.is_dir() {
            DirEntry::new_dir(
                |child_this| {
                    DirNode::new(Inode::new(
                        self.fs.clone(),
                        inode_ref.inode_num,
                        Some(child_this),
                    ))
                },
                reference,
            )
        } else {
            DirEntry::new_file(
                FileNode::new(Inode::new(self.fs.clone(), inode_ref.inode_num, None)),
                node_type,
                reference,
            )
        }
    }

    fn inode_ref(&self, fs: &Ext4, ino: u32) -> Ext4InodeRef {
        fs.get_inode_ref(ino)
    }

    fn lookup_locked(&self, fs: &mut Ext4, name: &str) -> VfsResult<DirEntry> {
        let mut result = Ext4DirSearchResult::new(Ext4DirEntry::default());
        fs.dir_find_entry(self.ino, name, &mut result)
            .map_err(into_vfs_err)?;
        let inode_ref = self.inode_ref(fs, result.dentry.inode);
        Ok(self.create_entry(&inode_ref, name))
    }
}

impl NodeOps for Inode {
    fn inode(&self) -> u64 {
        self.ino as u64
    }

    fn metadata(&self) -> VfsResult<Metadata> {
        let fs = self.fs.lock();
        let inode_ref = self.inode_ref(&fs, self.ino);
        let attr = FileAttr::from_inode_ref(&inode_ref);
        Ok(metadata_from_attr(&attr))
    }

    fn update_metadata(&self, update: MetadataUpdate) -> VfsResult<()> {
        let fs = self.fs.lock();
        let mut inode_ref = fs.get_inode_ref(self.ino);
        if let Some(mode) = update.mode {
            let kind = inode_ref.inode.mode() & 0xf000;
            inode_ref.inode.set_mode(kind | mode.bits());
        }
        if let Some((uid, gid)) = update.owner {
            inode_ref.inode.set_uid(uid as u16);
            inode_ref.inode.set_gid(gid as u16);
        }
        if let Some(atime) = update.atime {
            inode_ref.inode.set_atime(duration_to_ext4_time(atime));
        }
        if let Some(mtime) = update.mtime {
            inode_ref.inode.set_mtime(duration_to_ext4_time(mtime));
        }
        if let Some(now) = now_as_ext4_time() {
            inode_ref.inode.set_ctime(now);
        }
        fs.write_back_inode(&mut inode_ref);
        Ok(())
    }

    fn len(&self) -> VfsResult<u64> {
        let fs = self.fs.lock();
        Ok(fs.get_inode_ref(self.ino).inode.size())
    }

    fn filesystem(&self) -> &dyn FilesystemOps {
        &*self.fs
    }

    fn sync(&self, _data_only: bool) -> VfsResult<()> {
        Ok(())
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn flags(&self) -> NodeFlags {
        NodeFlags::BLOCKING
    }
}

impl FileNodeOps for Inode {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> VfsResult<usize> {
        self.fs
            .lock()
            .read_at(self.ino, offset as usize, buf)
            .map_err(into_vfs_err)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> VfsResult<usize> {
        self.fs
            .lock()
            .write_at(self.ino, offset as usize, buf)
            .map_err(into_vfs_err)
    }

    fn append(&self, buf: &[u8]) -> VfsResult<(usize, u64)> {
        let length = self.len()?;
        let written = self.write_at(buf, length)?;
        Ok((written, length + written as u64))
    }

    fn set_len(&self, len: u64) -> VfsResult<()> {
        let fs = self.fs.lock();
        let mut inode_ref = fs.get_inode_ref(self.ino);
        let old_len = inode_ref.inode.size();
        if len == old_len {
            return Ok(());
        }
        if len < old_len {
            fs.truncate_inode(&mut inode_ref, len)
                .map_err(into_vfs_err)?;
            return Ok(());
        }

        let mut remaining = len - old_len;
        let mut offset = old_len as usize;
        let zeros = vec![0; ext4_rs::BLOCK_SIZE];
        while remaining > 0 {
            let chunk = remaining.min(zeros.len() as u64) as usize;
            fs.write_at(self.ino, offset, &zeros[..chunk])
                .map_err(into_vfs_err)?;
            offset += chunk;
            remaining -= chunk as u64;
        }
        Ok(())
    }

    fn set_symlink(&self, target: &str) -> VfsResult<()> {
        self.set_len(0)?;
        self.fs
            .lock()
            .write_at(self.ino, 0, target.as_bytes())
            .map(|_| ())
            .map_err(into_vfs_err)
    }
}

impl Pollable for Inode {
    fn poll(&self) -> IoEvents {
        IoEvents::IN | IoEvents::OUT
    }

    fn register(&self, _context: &mut Context<'_>, _events: IoEvents) {}
}

impl DirNodeOps for Inode {
    fn read_dir(&self, offset: u64, sink: &mut dyn DirEntrySink) -> VfsResult<usize> {
        let fs = self.fs.lock();
        let entries = fs.dir_get_entries(self.ino);
        let mut count = 0usize;
        for (index, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            let name = entry.get_name();
            let inode_ref = fs.get_inode_ref(entry.inode);
            let node_type = into_vfs_type(inode_ref.inode.file_type());
            if !sink.accept(&name, entry.inode as u64, node_type, (index + 1) as u64) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    fn lookup(&self, name: &str) -> VfsResult<DirEntry> {
        let mut fs = self.fs.lock();
        self.lookup_locked(&mut fs, name)
    }

    fn create(
        &self,
        name: &str,
        node_type: NodeType,
        permission: NodePermission,
    ) -> VfsResult<DirEntry> {
        let inode_type = into_ext4_type(node_type)?;
        let mut fs = self.fs.lock();
        let mut existing = Ext4DirSearchResult::new(Ext4DirEntry::default());
        if fs.dir_find_entry(self.ino, name, &mut existing).is_ok() {
            return Err(VfsError::AlreadyExists);
        }
        let inode_ref = fs
            .create(self.ino, name, inode_type.bits() | permission.bits())
            .map_err(into_vfs_err)?;
        Ok(self.create_entry(&inode_ref, name))
    }

    fn link(&self, name: &str, node: &DirEntry) -> VfsResult<DirEntry> {
        let mut fs = self.fs.lock();
        let mut parent = fs.get_inode_ref(self.ino);
        let mut child = fs.get_inode_ref(node.inode() as u32);
        fs.link(&mut parent, &mut child, name)
            .map_err(into_vfs_err)?;
        fs.write_back_inode(&mut parent);
        fs.write_back_inode(&mut child);
        let linked = fs.get_inode_ref(child.inode_num);
        Ok(self.create_entry(&linked, name))
    }

    fn unlink(&self, name: &str) -> VfsResult<()> {
        let fs = self.fs.lock();
        let mut result = Ext4DirSearchResult::new(Ext4DirEntry::default());
        fs.dir_find_entry(self.ino, name, &mut result)
            .map_err(into_vfs_err)?;
        let mut parent = fs.get_inode_ref(self.ino);
        let mut child = fs.get_inode_ref(result.dentry.inode);
        if child.inode.is_dir() && fs.dir_has_entry(child.inode_num) {
            return Err(VfsError::ENOTEMPTY);
        }
        if child.inode.links_count() == 1 && child.inode.size() > 0 {
            fs.truncate_inode(&mut child, 0).map_err(into_vfs_err)?;
        }
        fs.unlink(&mut parent, &mut child, name)
            .map_err(into_vfs_err)?;
        Ok(())
    }

    fn rename(&self, src_name: &str, dst_dir: &DirNode, dst_name: &str) -> VfsResult<()> {
        let dst_dir: Arc<Self> = dst_dir.downcast().map_err(|_| VfsError::InvalidInput)?;
        let fs = self.fs.lock();

        let mut src_search = Ext4DirSearchResult::new(Ext4DirEntry::default());
        fs.dir_find_entry(self.ino, src_name, &mut src_search)
            .map_err(into_vfs_err)?;
        let src_inode = fs.get_inode_ref(src_search.dentry.inode);

        let mut dst_existing = Ext4DirSearchResult::new(Ext4DirEntry::default());
        match fs.dir_find_entry(dst_dir.ino, dst_name, &mut dst_existing) {
            Ok(_) => {
                if dst_existing.dentry.inode == src_inode.inode_num {
                    return Ok(());
                }
                let mut dst_parent = fs.get_inode_ref(dst_dir.ino);
                let mut dst_inode = fs.get_inode_ref(dst_existing.dentry.inode);
                if dst_inode.inode.is_dir() && fs.dir_has_entry(dst_inode.inode_num) {
                    return Err(VfsError::ENOTEMPTY);
                }
                if dst_inode.inode.links_count() == 1 && dst_inode.inode.size() > 0 {
                    fs.truncate_inode(&mut dst_inode, 0).map_err(into_vfs_err)?;
                }
                fs.unlink(&mut dst_parent, &mut dst_inode, dst_name)
                    .map_err(into_vfs_err)?;
            }
            Err(err) if err.error() == Errno::ENOENT => {}
            Err(err) => return Err(into_vfs_err(err)),
        }

        let mut dst_parent = fs.get_inode_ref(dst_dir.ino);
        fs.dir_add_entry(&mut dst_parent, &src_inode, dst_name)
            .map_err(into_vfs_err)?;

        if src_inode.inode.is_dir() && self.ino != dst_dir.ino {
            return Err(VfsError::OperationNotSupported);
        }

        fs.write_back_inode(&mut dst_parent);

        let mut src_parent = fs.get_inode_ref(self.ino);
        fs.dir_remove_entry(&mut src_parent, src_name)
            .map_err(into_vfs_err)?;
        Ok(())
    }
}
