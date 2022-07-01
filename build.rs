fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("api/proto/homekv_service.proto")?;
        Ok(())
}
