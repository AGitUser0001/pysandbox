use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let crate_root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let project_root = crate_root.join("../..");
    let component_root = project_root.join("component");
    let output_root = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let component_output = output_root.join("pysandbox.wasm");

    watch_tree(&component_root);
    watch_tree(&project_root.join("vendor/cbor2/cbor2"));

    let executable = componentize_py(&project_root);
    let status = Command::new(&executable)
        .current_dir(&component_root)
        .args([
            "-d",
            "wit",
            "-w",
            "python",
            "componentize",
            "guest",
            "-p",
            ".",
            "-p",
            "../vendor/cbor2",
            "-o",
        ])
        .arg(&component_output)
        .status()
        .unwrap_or_else(|error| {
            panic!(
                "failed to start {}: {error}",
                Path::new(&executable).display()
            )
        });
    assert!(status.success(), "componentize-py exited with {status}");

    let runtime_output = output_root.join("runtime");
    if runtime_output.exists() {
        fs::remove_dir_all(&runtime_output).expect("failed to clear generated runtime directory");
    }
    copy_tree(&component_root.join("runtime"), &runtime_output);
}

fn componentize_py(project_root: &Path) -> OsString {
    if let Some(executable) = env::var_os("PYSANDBOX_COMPONENTIZE_PY") {
        return executable;
    }

    let local = project_root.join(if cfg!(windows) {
        ".venv/Scripts/componentize-py.exe"
    } else {
        ".venv/bin/componentize-py"
    });
    if local.is_file() {
        return local.into_os_string();
    }

    "componentize-py".into()
}

fn watch_tree(root: &Path) {
    println!("cargo:rerun-if-changed={}", root.display());
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("failed to read build input").path();
        if path.is_dir() {
            watch_tree(&path);
        } else {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("failed to create generated runtime directory");
    for entry in fs::read_dir(source).expect("failed to read component runtime directory") {
        let path = entry
            .expect("failed to read component runtime entry")
            .path();
        let target = destination.join(path.file_name().expect("runtime entry has no filename"));
        if path.is_dir() {
            copy_tree(&path, &target);
        } else {
            fs::copy(&path, &target).expect("failed to copy component runtime file");
        }
    }
}
