fn main() {
    prost_build::compile_protos(&["../proto/nuefs.proto"], &["../proto/"]).unwrap();
}
