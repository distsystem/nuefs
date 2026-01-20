use pyo3_stub_gen::Result;

fn main() -> Result<()> {
    let stub = _nuefs::stub_info()?;
    stub.generate()?;
    Ok(())
}
