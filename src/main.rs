use protoc_rust;
fn main() {
    protoc_rust::Codegen::new()
        .out_dir("rs")
        .inputs(&["sparkplug_b.proto"])
        // .include("protos")
        .run()
        .expect("Running protoc failed.");
}
