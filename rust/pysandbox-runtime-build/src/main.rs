use std::path::PathBuf;

fn main() {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let output = arguments.next().map(PathBuf::from).unwrap_or_else(|| {
        eprintln!("usage: pysandbox-runtime-build <output-directory>");
        std::process::exit(2);
    });
    if arguments.next().is_some() {
        eprintln!("usage: pysandbox-runtime-build <output-directory>");
        std::process::exit(2);
    }

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let project_root = pysandbox_runtime_build::project_root_from_manifest(&manifest);
    pysandbox_runtime_build::generate(&project_root, &output);
}
