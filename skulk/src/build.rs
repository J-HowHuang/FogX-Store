use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["../predatorfox/src/cmd.proto"], &["src/"])?;
    Ok(())
}