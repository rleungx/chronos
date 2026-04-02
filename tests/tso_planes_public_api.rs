use std::any::TypeId;

use chronos::{TsoControlPlane, TsoDataPlane};

#[test]
fn crate_root_exports_plane_types() {
    let _ = TypeId::of::<TsoControlPlane>();
    let _ = TypeId::of::<TsoDataPlane>();
}
