fn main() {
    prost_build::compile_protos(&["proto/transfers.proto"], &["proto/"]).unwrap();
}
