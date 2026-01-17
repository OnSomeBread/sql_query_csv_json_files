use anyhow::Result;
use arrow::array::RecordBatch;
use datafusion::execution::context::SessionConfig;
use datafusion::{arrow::array::ArrayRef, prelude::SessionContext};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout},
    style::{Color, Style, Stylize},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
};
use rayon::prelude::*;
use rfd::FileDialog;

use crate::build_ctx;
pub struct App {
    curr_headers: Vec<String>,
    curr_command: String,
    curr_data: Vec<Vec<String>>,
    shown_data: (usize, usize),
    scroll_offset: (usize, usize),
    next_table_name: u8,
    ctx: SessionContext,
}

impl App {
    pub fn new() -> Self {
        let config = SessionConfig::new().with_information_schema(true);
        Self {
            curr_headers: vec![],
            curr_command: String::new(),
            curr_data: vec![],
            shown_data: (50, 8),
            scroll_offset: (0, 0),
            next_table_name: b'a',
            ctx: SessionContext::new_with_config(config),
        }
    }

    fn render(&self, frame: &mut Frame) {
        let layout = Layout::default()
            .direction(ratatui::layout::Direction::Vertical)
            .constraints(vec![Constraint::Percentage(85), Constraint::Min(3)])
            .split(frame.area());

        let headers = Row::new(
            self.curr_headers
                .iter()
                .skip(self.scroll_offset.1)
                .take(self.scroll_offset.1 + self.shown_data.1)
                .cloned(),
        )
        .style(Style::new().bold().fg(Color::LightBlue));

        let rows = self
            .curr_data
            .iter()
            .skip(self.scroll_offset.0)
            .take(self.scroll_offset.0 + self.shown_data.0)
            .map(|item| {
                Row::new(
                    item.iter()
                        .skip(self.scroll_offset.1)
                        .take(self.scroll_offset.1 + self.shown_data.1)
                        .cloned()
                        .map(Cell::from),
                )
                .style(Style::new().fg(Color::White))
            });

        frame.render_widget(
            Table::new(rows, (0..self.shown_data.1).map(|_| Constraint::Fill(1)))
                .header(headers)
                .block(Block::new().fg(Color::LightBlue).borders(Borders::ALL)),
            layout[0],
        );

        // command input
        frame.render_widget(
            Paragraph::new(self.curr_command.clone())
                .block(Block::new().fg(Color::LightBlue).borders(Borders::ALL)),
            layout[1],
        );
    }

    pub async fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        loop {
            terminal.draw(|frame| self.render(frame))?;
            if let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                match key.code {
                    KeyCode::Esc => return Ok(()),
                    KeyCode::Up => self.scroll_offset.0 = self.scroll_offset.0.saturating_sub(2),
                    KeyCode::Down => self.scroll_offset.0 = self.scroll_offset.0.saturating_add(2),
                    KeyCode::Left => self.scroll_offset.1 = self.scroll_offset.1.saturating_sub(1),
                    KeyCode::Right => self.scroll_offset.1 = self.scroll_offset.1.saturating_add(1),
                    KeyCode::Enter => self.parse_command().await?,
                    KeyCode::Backspace => {
                        let _ = self.curr_command.pop();
                    }
                    _ => {
                        if let Some(k) = key.code.as_char() {
                            self.curr_command.push(k);
                        }
                    }
                }
            }
        }
    }

    fn send_message(&mut self, error: String) {
        self.curr_headers = vec![];
        self.curr_data = vec![vec![error]];
        self.scroll_offset = (0, 0);
    }

    fn read_batch_to_array(batch: &RecordBatch) -> Vec<Vec<String>> {
        let columns: Vec<ArrayRef> = batch.columns().to_vec();
        let num_rows = batch.num_rows();
        let mut rows = Vec::with_capacity(num_rows);

        for row in 0..num_rows {
            let mut values = Vec::with_capacity(columns.len());

            for col in &columns {
                let value = if col.is_null(row) {
                    "null".to_string()
                } else {
                    arrow::util::display::array_value_to_string(col.as_ref(), row).unwrap()
                };
                values.push(value);
            }

            rows.push(values);
        }

        rows
    }

    async fn parse_command(&mut self) -> Result<()> {
        if self.curr_command.trim().eq_ignore_ascii_case("c") {
            self.create_table().await?;
        } else if self.curr_command.trim().eq_ignore_ascii_case("cls") {
            self.curr_data = vec![];
        } else {
            match self.ctx.sql(self.curr_command.trim_end()).await {
                Ok(df) => {
                    self.curr_headers = vec![];
                    for field in df.schema().fields() {
                        self.curr_headers.push(field.name().clone());
                    }

                    match df.collect().await {
                        Ok(batches) => {
                            self.curr_data = batches
                                .par_iter()
                                .map(Self::read_batch_to_array)
                                .flatten()
                                .collect();
                        }
                        Err(e) => self.send_message(e.to_string()),
                    }
                }
                Err(e) => self.send_message(e.to_string()),
            }
        }

        self.curr_command = String::new();
        self.scroll_offset = (0, 0);
        Ok(())
    }

    pub async fn create_table(&mut self) -> Result<()> {
        // user selects a view to view
        let Some(file_location) = FileDialog::new()
            .set_title("Select a CSV or JSON File")
            .add_filter(".csv or .json", &["csv", "json"])
            .pick_file()
        else {
            self.send_message("could not open file".to_string());
            return Ok(());
        };

        build_ctx::add_file_to_ctx(
            &self.ctx,
            char::from(self.next_table_name).to_string(),
            file_location.into_os_string().into_string().unwrap(),
        )
        .await?;

        self.send_message(format!(
            "created table {}",
            char::from(self.next_table_name)
        ));
        self.next_table_name += 1;

        Ok(())
    }
}
