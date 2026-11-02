use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The engine owns its wire contract under proto/ at the package root.
    let proto_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("proto");
    let protos = [
        proto_dir.join("execution_engine_vnext.proto"),
        proto_dir.join("execution_engine_v2.proto"),
    ];

    // Make sure proto changes trigger rebuilds (otherwise Cargo may reuse stale generated code).
    println!("cargo:rerun-if-changed={}", proto_dir.display());
    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    // Always use the protoc pinned by protoc-bin-vendored, never one from the
    // host, so generated code does not depend on what happens to be installed.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(&protos, &[proto_dir])?;
    Ok(())
}
