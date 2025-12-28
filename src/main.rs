use anyhow::Result;
use datafusion::prelude::SessionContext;
use rfd::FileDialog;
use tokio::io::AsyncBufReadExt;
use tracing::error;
mod build_ctx;
use datafusion::execution::context::SessionConfig;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // init non blocking loggin
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .without_time()
        .with_target(false)
        .init();

    // init the ctx
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    // user selects a view to view
    let Some(file_location) = FileDialog::new()
        .set_title("Select a CSV or JSON File")
        .add_filter(".csv or .json", &["csv", "json"])
        .pick_file()
    else {
        eprintln!("could not open file");
        return Ok(());
    };

    let table_name = b'a';
    build_ctx::add_file_to_ctx(
        &ctx,
        char::from(table_name).to_string(),
        file_location.into_os_string().into_string().unwrap(),
    )
    .await?;

    let stdin = tokio::io::stdin();
    let mut reader = tokio::io::BufReader::new(stdin);
    loop {
        let mut read_query = String::new();
        if let Err(e) = reader.read_line(&mut read_query).await {
            error!("{e}");
            continue;
        }

        if read_query.trim().eq_ignore_ascii_case("quit") {
            break;
        }

        print!("\x1B[2J\x1B[H");
        match ctx.sql(read_query.trim_end()).await {
            Ok(df) => {
                if let Err(e) = df.show().await {
                    error!("{e}");
                }
            }
            Err(e) => error!("{e}"),
        }
    }

    Ok(())
}
