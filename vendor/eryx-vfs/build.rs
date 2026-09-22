use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use wasmparser::{Parser, Payload};

const BOOTSTRAP_WASI_VERSION: &str = "0.2.9";
const WASI_VERSION_PLACEHOLDER: &str = "WASI_LIB_TARGET";

fn main() {
    println!("cargo:rerun-if-env-changed=PYSANDBOX_PREBUILT_RUNTIME");
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let output = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let source_wit = manifest.join("wit-template");
    let version = env::var_os("PYSANDBOX_PREBUILT_RUNTIME")
        .map(|root| {
            let root = PathBuf::from(root);
            let root = if root.is_absolute() {
                root
            } else {
                manifest.join("../..").join(root)
            };
            let component = root.join("pysandbox.wasm");
            println!("cargo:rerun-if-changed={}", component.display());
            component_filesystem_version(&component)
        })
        .unwrap_or_else(|| BOOTSTRAP_WASI_VERSION.to_owned());

    let generated_wit = output.join("wit");
    copy_wit(&source_wit, &generated_wit, &version);
    for module in ["bindings", "hybrid_bindings"] {
        let source = manifest.join("src").join(format!("{module}.rs"));
        println!("cargo:rerun-if-changed={}", source.display());
        let source_text = fs::read_to_string(&source).unwrap();
        let path = format!("{:?}", generated_wit.to_str().unwrap());
        let generated = source_text.replace("path: \"wit\"", &format!("path: {path}"));
        assert_ne!(
            generated, source_text,
            "missing bindgen WIT path in {module}"
        );
        fs::write(output.join(format!("{module}.rs")), generated).unwrap();
    }
    let declarations = ["bindings", "hybrid_bindings"]
        .into_iter()
        .map(|module| {
            let path = output.join(format!("{module}.rs"));
            format!("#[path = {:?}] mod {module};\n", path.to_str().unwrap())
        })
        .collect::<String>();
    fs::write(output.join("binding_modules.rs"), declarations).unwrap();
    println!("cargo:rustc-env=ERYX_VFS_FILESYSTEM_WASI_VERSION={version}");
}

fn component_filesystem_version(path: &Path) -> String {
    let bytes = fs::read(path).unwrap();
    let mut depth = 0;
    let mut version = None;
    for payload in Parser::new(0).parse_all(&bytes) {
        match payload.unwrap() {
            Payload::ComponentImportSection(section) if depth == 0 => {
                for import in section {
                    let import = import.unwrap();
                    if let Some(found) = import.name.name.strip_prefix("wasi:filesystem/preopens@")
                    {
                        assert!(
                            version.replace(found.to_owned()).is_none(),
                            "duplicate filesystem preopens import"
                        );
                    }
                }
            }
            Payload::ModuleSection { .. } | Payload::ComponentSection { .. } => depth += 1,
            Payload::End(_) if depth > 0 => depth -= 1,
            _ => {}
        }
    }
    version.expect("component must import wasi:filesystem/preopens")
}

fn copy_wit(source: &Path, destination: &Path, version: &str) {
    fs::create_dir_all(destination).unwrap();
    println!("cargo:rerun-if-changed={}", source.display());
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = destination.join(entry.file_name());
        if path.is_dir() {
            copy_wit(&path, &target, version);
        } else {
            let text = fs::read_to_string(&path).unwrap();
            fs::write(target, text.replace(WASI_VERSION_PLACEHOLDER, version)).unwrap();
        }
    }
}
