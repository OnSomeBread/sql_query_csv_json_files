use anyhow::{Result, anyhow};
use datafusion::prelude::*;
use std::io::Write;
pub async fn add_file_to_ctx(
    ctx: &SessionContext,
    table_name: String,
    file_name: String,
) -> Result<()> {
    if let Some(file_format) = file_name.clone().split('.').next_back() {
        if file_format == "csv" {
            ctx.register_csv(table_name.clone(), file_name, CsvReadOptions::new())
                .await?;
        } else if file_format == "json" {
            if ctx
                .register_json(
                    table_name.clone(),
                    file_name.clone(),
                    NdJsonReadOptions::default(),
                )
                .await
                .is_err()
            {
                let p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                let v: serde_json::Value =
                    serde_json::from_str(&std::fs::read_to_string(file_name)?)?;
                if let Some(array) = v.as_array() {
                    let mut writer = std::io::Cursor::new(Vec::new());

                    for item in array {
                        serde_json::to_writer(&mut writer, item)?;
                        writer.write_all(b"\n")?;
                    }

                    let new_file = format!("{}{}", p.to_string_lossy(), "temp.json");
                    std::fs::write(new_file.clone(), writer.into_inner())?;

                    ctx.register_json(table_name.clone(), new_file, NdJsonReadOptions::default())
                        .await?;
                } else {
                    return Err(anyhow!("could not parse from json array or ndjson"));
                }
            }
        } else {
            return Err(anyhow!("cannot parse this file format"));
        }
    } else {
        return Err(anyhow!("could not parse file format"));
    }

    tracing::info!("created table {}", table_name);
    Ok(())
}
