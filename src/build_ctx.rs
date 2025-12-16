use datafusion::{error::DataFusionError, prelude::*};
pub async fn add_file_to_ctx(
    ctx: &SessionContext,
    table_name: String,
    file_name: String,
) -> datafusion::error::Result<()> {
    if let Some(file_format) = file_name.clone().split('.').next_back() {
        if file_format == "csv" {
            ctx.register_csv(table_name.clone(), file_name, CsvReadOptions::new())
                .await?;
        } else if file_format == "json" {
            ctx.register_json(table_name.clone(), file_name, NdJsonReadOptions::default())
                .await?;
        } else {
            return Err(DataFusionError::Configuration(
                "could not parse file format".into(),
            ));
        }
    } else {
        return Err(DataFusionError::Configuration(
            "could not parse file format".into(),
        ));
    }

    tracing::info!("created table {}", table_name);
    Ok(())
}
