use crate::error::{Error, Result};
use crate::proto::hadoop::ozone::{self, ozone_acl_info, ozone_obj, OzoneAclInfo, OzoneObj};
use std::fmt::Display;

const ACL_READ: u16 = 1 << 0;
const ACL_WRITE: u16 = 1 << 1;
const ACL_CREATE: u16 = 1 << 2;
const ACL_LIST: u16 = 1 << 3;
const ACL_DELETE: u16 = 1 << 4;
const ACL_ALL: u16 = 1 << 7;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AclEntryType {
    User,
    Group,
    Mask,
    Other,
}

impl Display for AclEntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                AclEntryType::User => "user",
                AclEntryType::Group => "group",
                AclEntryType::Mask => "mask",
                AclEntryType::Other => "other",
            }
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AclEntryScope {
    Access,
    Default,
}

impl Display for AclEntryScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                AclEntryScope::Access => "access",
                AclEntryScope::Default => "default",
            }
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FsAction {
    None = 0,
    Execute = 1,
    Write = 2,
    WriteExecute = 3,
    Read = 4,
    ReadExecute = 5,
    ReadWrite = 6,
    PermAll = 7,
}

impl Display for FsAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                FsAction::None => "---",
                FsAction::Execute => "--x",
                FsAction::Write => "-w-",
                FsAction::WriteExecute => "-wx",
                FsAction::Read => "r--",
                FsAction::ReadExecute => "r-x",
                FsAction::ReadWrite => "rw-",
                FsAction::PermAll => "rwx",
            }
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclEntry {
    pub r#type: AclEntryType,
    pub scope: AclEntryScope,
    pub permissions: FsAction,
    pub name: Option<String>,
}

impl AclEntry {
    pub fn new(
        r#type: impl Into<AclEntryType>,
        scope: impl Into<AclEntryScope>,
        permissions: impl Into<FsAction>,
        name: Option<String>,
    ) -> Self {
        Self {
            r#type: r#type.into(),
            scope: scope.into(),
            permissions: permissions.into(),
            name,
        }
    }

    pub fn is_default(&self) -> bool {
        self.scope == AclEntryScope::Default
    }

    pub(crate) fn to_ozone(&self) -> OzoneAclInfo {
        OzoneAclInfo {
            r#type: ozone_acl_type(&self.r#type) as i32,
            name: ozone_acl_name(&self.r#type, self.name.as_deref()),
            rights: rights_to_bytes(fs_action_to_rights(&self.permissions)),
            acl_scope: match self.scope {
                AclEntryScope::Access => ozone_acl_info::OzoneAclScope::Access as i32,
                AclEntryScope::Default => ozone_acl_info::OzoneAclScope::Default as i32,
            },
        }
    }

    pub(crate) fn from_ozone(value: OzoneAclInfo) -> Result<Self> {
        let r#type = match ozone_acl_info::OzoneAclType::try_from(value.r#type)
            .map_err(|_| Error::InvalidState(format!("unknown Ozone ACL type {}", value.r#type)))?
        {
            ozone_acl_info::OzoneAclType::User => AclEntryType::User,
            ozone_acl_info::OzoneAclType::Group => AclEntryType::Group,
            ozone_acl_info::OzoneAclType::World | ozone_acl_info::OzoneAclType::Anonymous => {
                AclEntryType::Other
            }
            ozone_acl_info::OzoneAclType::ClientIp => AclEntryType::Other,
        };
        let scope =
            match ozone_acl_info::OzoneAclScope::try_from(value.acl_scope).map_err(|_| {
                Error::InvalidState(format!("unknown Ozone ACL scope {}", value.acl_scope))
            })? {
                ozone_acl_info::OzoneAclScope::Access => AclEntryScope::Access,
                ozone_acl_info::OzoneAclScope::Default => AclEntryScope::Default,
            };
        Ok(Self {
            r#type,
            scope,
            permissions: rights_to_fs_action(bytes_to_rights(&value.rights)),
            name: if value.name.is_empty() || value.name == "WORLD" || value.name == "ANONYMOUS" {
                None
            } else {
                Some(value.name)
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclStatus {
    pub owner: String,
    pub group: String,
    pub sticky: bool,
    pub entries: Vec<AclEntry>,
    pub permission: u16,
}

impl AclStatus {
    pub(crate) fn from_ozone(acls: Vec<ozone::OzoneAclInfo>) -> Result<Self> {
        Ok(Self {
            owner: String::new(),
            group: String::new(),
            sticky: false,
            entries: acls
                .into_iter()
                .map(AclEntry::from_ozone)
                .collect::<Result<Vec<_>>>()?,
            permission: 0,
        })
    }
}

pub(crate) fn key_obj(volume: &str, bucket: &str, key: &str) -> OzoneObj {
    if key.is_empty() {
        OzoneObj {
            res_type: ozone_obj::ObjectType::Bucket as i32,
            store_type: ozone_obj::StoreType::Ozone as i32,
            path: format!("/{volume}/{bucket}"),
        }
    } else {
        OzoneObj {
            res_type: ozone_obj::ObjectType::Key as i32,
            store_type: ozone_obj::StoreType::Ozone as i32,
            path: format!("/{volume}/{bucket}/{key}"),
        }
    }
}

fn ozone_acl_type(r#type: &AclEntryType) -> ozone_acl_info::OzoneAclType {
    match r#type {
        AclEntryType::User => ozone_acl_info::OzoneAclType::User,
        AclEntryType::Group => ozone_acl_info::OzoneAclType::Group,
        AclEntryType::Mask => ozone_acl_info::OzoneAclType::Group,
        AclEntryType::Other => ozone_acl_info::OzoneAclType::World,
    }
}

fn ozone_acl_name(r#type: &AclEntryType, name: Option<&str>) -> String {
    match r#type {
        AclEntryType::Other => "WORLD".to_string(),
        _ => name.unwrap_or("").to_string(),
    }
}

fn fs_action_to_rights(action: &FsAction) -> u16 {
    match action {
        FsAction::None => 0,
        FsAction::Execute => ACL_LIST,
        FsAction::Write => ACL_WRITE | ACL_CREATE | ACL_DELETE,
        FsAction::WriteExecute => ACL_WRITE | ACL_CREATE | ACL_DELETE | ACL_LIST,
        FsAction::Read => ACL_READ | ACL_LIST,
        FsAction::ReadExecute => ACL_READ | ACL_LIST,
        FsAction::ReadWrite => ACL_READ | ACL_LIST | ACL_WRITE | ACL_CREATE | ACL_DELETE,
        FsAction::PermAll => ACL_ALL,
    }
}

fn rights_to_fs_action(rights: u16) -> FsAction {
    if rights & ACL_ALL != 0 {
        return FsAction::PermAll;
    }
    let read = rights & (ACL_READ | ACL_LIST) != 0;
    let write = rights & (ACL_WRITE | ACL_CREATE | ACL_DELETE) != 0;
    match (read, write) {
        (true, true) => FsAction::ReadWrite,
        (true, false) => FsAction::Read,
        (false, true) => FsAction::Write,
        (false, false) => FsAction::None,
    }
}

fn rights_to_bytes(rights: u16) -> Vec<u8> {
    if rights == 0 {
        return vec![0];
    }
    let mut bytes = rights.to_le_bytes().to_vec();
    while bytes.last() == Some(&0) {
        bytes.pop();
    }
    bytes
}

fn bytes_to_rights(bytes: &[u8]) -> u16 {
    let mut value = 0u16;
    for (index, byte) in bytes.iter().take(2).enumerate() {
        value |= (*byte as u16) << (index * 8);
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_entry_roundtrips_through_ozone_bits() {
        let entry = AclEntry::new(
            AclEntryType::User,
            AclEntryScope::Access,
            FsAction::ReadWrite,
            Some("alice".to_string()),
        );

        let ozone = entry.to_ozone();

        assert_eq!(ozone.r#type, ozone_acl_info::OzoneAclType::User as i32);
        assert_eq!(ozone.name, "alice");
        assert_eq!(
            bytes_to_rights(&ozone.rights),
            ACL_READ | ACL_LIST | ACL_WRITE | ACL_CREATE | ACL_DELETE
        );

        let roundtrip = AclEntry::from_ozone(ozone).unwrap();

        assert_eq!(roundtrip, entry);
    }

    #[test]
    fn other_acl_maps_to_world_acl() {
        let entry = AclEntry::new(
            AclEntryType::Other,
            AclEntryScope::Default,
            FsAction::Read,
            None,
        );

        let ozone = entry.to_ozone();

        assert_eq!(ozone.r#type, ozone_acl_info::OzoneAclType::World as i32);
        assert_eq!(ozone.name, "WORLD");
        assert_eq!(
            ozone.acl_scope,
            ozone_acl_info::OzoneAclScope::Default as i32
        );

        let roundtrip = AclEntry::from_ozone(ozone).unwrap();

        assert_eq!(roundtrip.r#type, AclEntryType::Other);
        assert_eq!(roundtrip.name, None);
        assert_eq!(roundtrip.permissions, FsAction::Read);
    }

    #[test]
    fn key_acl_object_uses_ozone_key_path() {
        let obj = key_obj("vol", "bucket", "dir/file.txt");

        assert_eq!(obj.res_type, ozone_obj::ObjectType::Key as i32);
        assert_eq!(obj.store_type, ozone_obj::StoreType::Ozone as i32);
        assert_eq!(obj.path, "/vol/bucket/dir/file.txt");
    }
}
