use color_eyre::Result;

mod app;
mod build_ctx;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .without_time()
        .with_target(false)
        .init();
    color_eyre::install()?;

    let mut terminal = ratatui::init();
    let mut app = crate::app::App::new();
    app.create_table().await?;
    let result = app.run(&mut terminal).await;
    ratatui::restore();
    result
}
