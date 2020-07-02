fn main() {
    let res = prost_build::compile_protos(
        &["src/protocol.proto", "src/bin/server/state.proto"],
        &["src/"],
    );

    match res {
        Ok(_) => {}

        Err(err) => {
            panic!("protoc failed!\n{}", err);
        }
    }

    println!("cargo:rerun-if-changed=src/protocol.proto");
    println!("cargo:rerun-if-changed=src/bin/server/state.proto");
}
