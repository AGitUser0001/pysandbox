use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub fn generate(project_root: &Path, output_root: &Path) {
    let output_root = if output_root.is_absolute() {
        output_root.to_owned()
    } else {
        project_root.join(output_root)
    };
    fs::create_dir_all(&output_root).expect("failed to create runtime artifact directory");

    let runtime_output = output_root.join("runtime");
    if runtime_output.exists() {
        fs::remove_dir_all(&runtime_output).expect("failed to clear generated runtime directory");
    }
    copy_python_stdlib(&project_root.join("vendor/cpython/Lib"), &runtime_output);
    copy_tree(
        &project_root.join("vendor/cbor2/cbor2"),
        &runtime_output.join("lib/python3.14/site-packages/cbor2"),
    );
    fs::copy(
        project_root.join("vendor/cpython/LICENSE"),
        runtime_output.join("LICENSE"),
    )
    .expect("failed to copy the CPython license");

    let component_root = project_root.join("component");
    let component_output = output_root.join("pysandbox.wasm");
    let executable = componentize_py(project_root);
    let status = Command::new(&executable)
        .current_dir(&component_root)
        .args([
            "-d",
            "wit",
            "-w",
            "python",
            "componentize",
            "main",
            "-p",
            "src",
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

    tokio::runtime::Runtime::new()
        .expect("failed to create build runtime")
        .block_on(pysandbox_sandboxd::component_worker::compile_python_root(
            &component_output,
            &runtime_output,
        ))
        .expect("failed to compile the Python standard library with WASI");
}

pub fn project_root_from_manifest(manifest_dir: &Path) -> PathBuf {
    manifest_dir.join("../..")
}

fn componentize_py(project_root: &Path) -> OsString {
    if let Some(executable) = std::env::var_os("PYSANDBOX_COMPONENTIZE_PY") {
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

    OsStr::new("componentize-py").to_owned()
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("failed to create generated runtime directory");
    for entry in fs::read_dir(source).expect("failed to read component runtime directory") {
        let path = entry.expect("failed to read runtime entry").path();
        let target = destination.join(path.file_name().expect("runtime entry has no filename"));
        if path.is_dir() {
            copy_tree(&path, &target);
        } else {
            fs::copy(&path, &target).expect("failed to copy runtime file");
        }
    }
}

fn copy_python_stdlib(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("failed to create generated Python runtime directory");
    for entry in fs::read_dir(source).expect("failed to read CPython standard library") {
        let path = entry
            .expect("failed to read CPython standard library entry")
            .path();
        if path.file_name().is_some_and(|name| name == "test") {
            continue;
        }
        let target = destination.join(path.file_name().expect("stdlib entry has no filename"));
        if path.is_dir() {
            copy_tree(&path, &target);
        } else {
            fs::copy(&path, &target).expect("failed to copy CPython standard library file");
        }
    }
}
