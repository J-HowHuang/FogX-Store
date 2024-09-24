use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/catalog.proto", "src/cmd.proto"], &["src/"])?;
    Ok(())
}