use std::fs;

use super::*;
use crate::command_router::CommandRouter;
use crate::device_registry::DeviceRegistry;
use crate::port_worker::DeviceInstance;
use crate::processing::Management;
use crate::virtual_device::{ThermalPlantModel, VirtualDriver};

fn fixture() -> (tempfile::TempDir, PathBuf, DeviceService) {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("experiment.toml");
    fs::write(
        &target,
        "# Original experiment\nschema_version = 1\ncomponents = []\n",
    )
    .unwrap();
    let mut registry = DeviceRegistry::new();
    registry
        .register_unavailable_port("virtual", "fixture")
        .unwrap();
    let model = ThermalPlantModel::from_options(&toml::Table::new()).unwrap();
    let instance = DeviceInstance::new("furnace1", VirtualDriver::new(&model)).unwrap();
    registry.register_device("virtual", &instance).unwrap();
    let mut service = DeviceService::new(CommandRouter::new(registry), 8);
    service.experiment_file = Some(target.clone());
    (directory, target, service)
}

#[test]
fn saves_empty_graph_and_repeats_without_changing_revision_or_bytes() {
    let (directory, target, service) = fixture();
    let original = fs::read(&target).unwrap();
    assert_eq!(
        service.manage(Management::Save {}).unwrap(),
        json!({"revision":0})
    );
    let saved = fs::read_to_string(&target).unwrap();
    assert_ne!(saved.as_bytes(), original);
    let document: toml::Table = saved.parse().unwrap();
    assert_eq!(document["schema_version"].as_integer(), Some(1));
    assert!(document["components"].as_array().unwrap().is_empty());
    assert_eq!(
        service.manage(Management::Save {}).unwrap(),
        json!({"revision":0})
    );
    assert_eq!(fs::read_to_string(&target).unwrap(), saved);
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[test]
fn exports_the_whole_graph_beyond_the_default_management_page() {
    let (_directory, target, service) = fixture();
    for revision in 0..20 {
        service
            .manage(Management::Create {
                configuration: serde_json::from_value(json!({
                    "name":format!("ema{revision}"), "type_id":"ema",
                    "inputs":{"input":"furnace1/sample_temperature"}, "max_age_ms":5000
                }))
                .unwrap(),
                expected_revision: revision,
            })
            .unwrap();
    }
    assert_eq!(
        service.manage(Management::Save {}).unwrap(),
        json!({"revision":20})
    );
    let document: toml::Table = fs::read_to_string(&target).unwrap().parse().unwrap();
    assert_eq!(document["components"].as_array().unwrap().len(), 20);
}

#[test]
fn stale_staged_snapshot_cannot_replace_a_newer_save() {
    let (directory, target, service) = fixture();
    let stale = {
        let _gate = service.lock_service(GateOperation::Manage);
        service.capture_experiment(None).unwrap()
    }
    .stage()
    .unwrap();
    let temporary = stale.temporary.to_path_buf();
    service.manage(Management::Create {
        configuration: serde_json::from_value(json!({"name":"smooth","type_id":"ema","inputs":{"input":"furnace1/sample_temperature"},"max_age_ms":5000})).unwrap(),
        expected_revision: 0,
    }).unwrap();
    assert_eq!(
        service.manage(Management::Save {}).unwrap(),
        json!({"revision":1})
    );
    let newest = fs::read_to_string(&target).unwrap();
    assert!(newest.contains("smooth"));
    assert_eq!(
        service.publish_experiment(stale),
        Err("revision_conflict".into())
    );
    assert!(!temporary.exists());
    assert_eq!(fs::read_to_string(&target).unwrap(), newest);
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[cfg(windows)]
#[test]
fn windows_sharing_violation_preserves_original_and_cleans_up() {
    use std::os::windows::fs::OpenOptionsExt;

    let (directory, target, service) = fixture();
    let original = fs::read(&target).unwrap();
    // FILE_SHARE_READ alone denies replacement while allowing readers.
    let held = fs::OpenOptions::new()
        .read(true)
        .share_mode(1)
        .open(&target)
        .unwrap();
    assert_eq!(
        service.manage(Management::Save {}),
        Err("experiment_write_failed".into())
    );
    drop(held);
    assert_eq!(fs::read(&target).unwrap(), original);
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    service.manage(Management::Save {}).unwrap();
    assert_ne!(fs::read(&target).unwrap(), original);
}
