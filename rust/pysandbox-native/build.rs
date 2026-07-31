use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let crate_root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let project_root = crate_root.join("../..");
    let output_root = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    println!("cargo:rerun-if-env-changed=PYSANDBOX_PREBUILT_RUNTIME");

    if let Some(prebuilt) = env::var_os("PYSANDBOX_PREBUILT_RUNTIME") {
        let prebuilt = PathBuf::from(prebuilt);
        let prebuilt = if prebuilt.is_absolute() {
            prebuilt
        } else {
            project_root.join(prebuilt)
        };
        watch_tree(&prebuilt);
        copy_prebuilt_runtime(&prebuilt, &output_root);
        return;
    }

    watch_generation_inputs(&project_root);

    #[cfg(feature = "generate-runtime")]
    {
        pysandbox_runtime_build::generate(&project_root, &output_root);
        return;
    }

    #[cfg(not(feature = "generate-runtime"))]
    panic!(
        "runtime generation is disabled; set PYSANDBOX_PREBUILT_RUNTIME \
         to a directory containing pysandbox.wasm and runtime/"
    );
}

fn copy_prebuilt_runtime(source: &Path, destination: &Path) {
    let component = source.join("pysandbox.wasm");
    let runtime = source.join("runtime");
    assert!(
        component.is_file() && runtime.is_dir(),
        "prebuilt runtime must contain pysandbox.wasm and runtime/: {}",
        source.display()
    );

    fs::create_dir_all(destination).expect("failed to create Cargo output directory");
    fs::copy(component, destination.join("pysandbox.wasm"))
        .expect("failed to copy prebuilt component");
    let runtime_destination = destination.join("runtime");
    if runtime_destination.exists() {
        fs::remove_dir_all(&runtime_destination)
            .expect("failed to clear Cargo runtime output directory");
    }
    copy_tree(&runtime, &runtime_destination);
}

fn watch_generation_inputs(project_root: &Path) {
    println!("cargo:rerun-if-env-changed=PYSANDBOX_COMPONENTIZE_PY");
    watch_tree(&project_root.join("component"));
    watch_tree(&project_root.join("vendor/cbor2/cbor2"));
    watch_python_stdlib(&project_root.join("vendor/cpython/Lib"));
    println!(
        "cargo:rerun-if-changed={}",
        project_root.join("vendor/cpython/LICENSE").display()
    );
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

fn watch_python_stdlib(root: &Path) {
    println!("cargo:rerun-if-changed={}", root.display());
    for entry in fs::read_dir(root).expect("failed to read CPython standard library") {
        let path = entry
            .expect("failed to read CPython standard library entry")
            .path();
        if path.file_name().is_some_and(|name| name == "test") {
            continue;
        }
        if path.is_dir() {
            watch_tree(&path);
        } else {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("failed to create runtime output directory");
    for entry in fs::read_dir(source).expect("failed to read prebuilt runtime directory") {
        let path = entry.expect("failed to read prebuilt runtime entry").path();
        let target = destination.join(path.file_name().expect("runtime entry has no filename"));
        if path.is_dir() {
            copy_tree(&path, &target);
        } else {
            fs::copy(&path, &target).expect("failed to copy prebuilt runtime file");
        }
    }
}
