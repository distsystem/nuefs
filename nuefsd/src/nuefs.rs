// @generated
// -- Data types --

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ManifestEntry {
    #[prost(string, tag="1")]
    pub virtual_path: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub backend_path: ::prost::alloc::string::String,
    #[prost(bool, tag="3")]
    pub is_dir: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MountRoot {
    #[prost(string, tag="1")]
    pub virtual_prefix: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub backend_path: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OwnerInfo {
    #[prost(string, tag="1")]
    pub owner: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub backend_path: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MountStatus {
    #[prost(uint64, tag="1")]
    pub mount_id: u64,
    #[prost(string, tag="2")]
    pub root: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DaemonInfo {
    #[prost(uint32, tag="1")]
    pub pid: u32,
    #[prost(string, tag="2")]
    pub socket: ::prost::alloc::string::String,
    #[prost(uint64, tag="3")]
    pub started_at: u64,
}
// -- Requests --

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Request {
    #[prost(oneof="request::Method", tags="1, 2, 3, 4, 5, 6, 7, 8")]
    pub method: ::core::option::Option<request::Method>,
}
/// Nested message and enum types in `Request`.
pub mod request {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Method {
        #[prost(message, tag="1")]
        Mount(super::MountReq),
        #[prost(message, tag="2")]
        Unmount(super::UnmountReq),
        #[prost(message, tag="3")]
        Which(super::WhichReq),
        #[prost(message, tag="4")]
        Status(super::StatusReq),
        #[prost(message, tag="5")]
        DaemonInfo(super::DaemonInfoReq),
        #[prost(message, tag="6")]
        Update(super::UpdateReq),
        #[prost(message, tag="7")]
        Resolve(super::ResolveReq),
        #[prost(message, tag="8")]
        Shutdown(super::ShutdownReq),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MountReq {
    #[prost(string, tag="1")]
    pub root: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub entries: ::prost::alloc::vec::Vec<ManifestEntry>,
    #[prost(message, repeated, tag="3")]
    pub mount_roots: ::prost::alloc::vec::Vec<MountRoot>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnmountReq {
    #[prost(uint64, tag="1")]
    pub mount_id: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WhichReq {
    #[prost(uint64, tag="1")]
    pub mount_id: u64,
    #[prost(string, tag="2")]
    pub path: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusReq {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DaemonInfoReq {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateReq {
    #[prost(uint64, tag="1")]
    pub mount_id: u64,
    #[prost(message, repeated, tag="2")]
    pub entries: ::prost::alloc::vec::Vec<ManifestEntry>,
    #[prost(message, repeated, tag="3")]
    pub mount_roots: ::prost::alloc::vec::Vec<MountRoot>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolveReq {
    #[prost(string, tag="1")]
    pub root: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShutdownReq {
}
// -- Responses --

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Response {
    #[prost(oneof="response::Result", tags="1, 2")]
    pub result: ::core::option::Option<response::Result>,
}
/// Nested message and enum types in `Response`.
pub mod response {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag="1")]
        Ok(super::OkPayload),
        #[prost(string, tag="2")]
        Error(::prost::alloc::string::String),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OkPayload {
    #[prost(oneof="ok_payload::Value", tags="1, 2, 3, 4, 5, 6")]
    pub value: ::core::option::Option<ok_payload::Value>,
}
/// Nested message and enum types in `OkPayload`.
pub mod ok_payload {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(uint64, tag="1")]
        MountId(u64),
        #[prost(message, tag="2")]
        OwnerInfo(super::OwnerInfo),
        #[prost(message, tag="3")]
        Status(super::StatusList),
        #[prost(message, tag="4")]
        DaemonInfo(super::DaemonInfo),
        #[prost(message, tag="5")]
        Resolve(super::ResolveResult),
        #[prost(message, tag="6")]
        Empty(super::Empty),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusList {
    #[prost(message, repeated, tag="1")]
    pub mounts: ::prost::alloc::vec::Vec<MountStatus>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolveResult {
    #[prost(uint64, optional, tag="1")]
    pub mount_id: ::core::option::Option<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Empty {
}
// @@protoc_insertion_point(module)
