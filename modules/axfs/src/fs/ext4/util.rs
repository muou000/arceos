use core::time::Duration;

use axerrno::LinuxError;
use axfs_ng_vfs::{Metadata, NodePermission, NodeType, VfsError};
use ext4_rs::{Ext4Error, Ext4InodeRef, FileAttr, InodeFileType};

pub fn into_vfs_err(err: Ext4Error) -> VfsError {
    let linux_error = LinuxError::try_from(err.error() as i32).unwrap_or(LinuxError::EIO);
    VfsError::from(linux_error).canonicalize()
}

pub fn into_vfs_type(ty: InodeFileType) -> NodeType {
    match ty {
        InodeFileType::S_IFREG => NodeType::RegularFile,
        InodeFileType::S_IFDIR => NodeType::Directory,
        InodeFileType::S_IFCHR => NodeType::CharacterDevice,
        InodeFileType::S_IFBLK => NodeType::BlockDevice,
        InodeFileType::S_IFIFO => NodeType::Fifo,
        InodeFileType::S_IFSOCK => NodeType::Socket,
        InodeFileType::S_IFLNK => NodeType::Symlink,
        _ => NodeType::Unknown,
    }
}

pub fn into_ext4_type(ty: NodeType) -> Result<InodeFileType, VfsError> {
    Ok(match ty {
        NodeType::Fifo => InodeFileType::S_IFIFO,
        NodeType::CharacterDevice => InodeFileType::S_IFCHR,
        NodeType::Directory => InodeFileType::S_IFDIR,
        NodeType::BlockDevice => InodeFileType::S_IFBLK,
        NodeType::RegularFile => InodeFileType::S_IFREG,
        NodeType::Symlink => InodeFileType::S_IFLNK,
        NodeType::Socket => InodeFileType::S_IFSOCK,
        NodeType::Unknown => return Err(VfsError::InvalidData),
    })
}

pub fn metadata_from_attr(attr: &FileAttr) -> Metadata {
    Metadata {
        inode: attr.ino,
        device: 0,
        nlink: attr.nlink as u64,
        mode: NodePermission::from_bits_truncate(attr.perm.bits()),
        node_type: into_vfs_type(attr.kind),
        uid: attr.uid,
        gid: attr.gid,
        size: attr.size,
        block_size: attr.blksize as u64,
        blocks: attr.blocks,
        rdev: Default::default(),
        atime: Duration::from_secs(attr.atime as u64),
        mtime: Duration::from_secs(attr.mtime as u64),
        ctime: Duration::from_secs(attr.ctime as u64),
    }
}

pub fn metadata_from_inode_ref(inode_ref: &Ext4InodeRef) -> Metadata {
    metadata_from_attr(&FileAttr::from_inode_ref(inode_ref))
}

pub fn duration_to_ext4_time(value: Duration) -> u32 {
    value.as_secs().min(u32::MAX as u64) as u32
}

pub fn now_as_ext4_time() -> Option<u32> {
    if cfg!(feature = "times") {
        Some(duration_to_ext4_time(axhal::time::wall_time()))
    } else {
        None
    }
}
