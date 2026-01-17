use anyhow::Result;

mod app;
mod build_ctx;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let mut terminal = ratatui::init();
    let mut app = crate::app::App::new();
    app.create_table().await?;
    let result = app.run(&mut terminal).await;
    ratatui::restore();
    result
}
