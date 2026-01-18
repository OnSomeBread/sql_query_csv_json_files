use anyhow::{Result, anyhow};
use arrow::array::RecordBatch;
use datafusion::execution::context::SessionConfig;
use datafusion::{arrow::array::ArrayRef, prelude::SessionContext};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::text::{Line, Span};
use ratatui::widgets::BorderType;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout},
    style::{Color, Style, Stylize},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
};
use rayon::prelude::*;
use rfd::FileDialog;
use std::collections::VecDeque;

use crate::build_ctx;
pub struct App {
    file_names: Vec<String>,
    command_history: VecDeque<String>,
    command_history_idx: usize,
    cursor: usize,
    curr_headers: Vec<String>,
    curr_command: String,
    curr_data: Vec<Vec<String>>,
    scroll_offset: (usize, usize),
    next_table_name: u8,
    ctx: SessionContext,
}

impl App {
    pub fn new() -> Self {
        let config = SessionConfig::new().with_information_schema(true);
        Self {
            file_names: vec![],
            command_history: VecDeque::new(),
            command_history_idx: 0,
            cursor: 0,
            curr_headers: vec![],
            curr_command: String::new(),
            curr_data: vec![],
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

        let size = layout[0].as_size();
        let (width, height) = (
            (size.width / 24) as usize,
            (size.height.saturating_sub(3)) as usize,
        );

        let headers = Row::new(
            self.curr_headers
                .iter()
                .skip(self.scroll_offset.1)
                .take(self.scroll_offset.1 + width)
                .cloned(),
        )
        .style(Style::new().bold().fg(Color::LightBlue));

        let rows = self
            .curr_data
            .iter()
            .skip(self.scroll_offset.0)
            .take(self.scroll_offset.0 + height)
            .map(|item| {
                Row::new(
                    item.iter()
                        .skip(self.scroll_offset.1)
                        .take(self.scroll_offset.1 + width)
                        .cloned()
                        .map(Cell::from),
                )
                .style(Style::new().fg(Color::White))
            });

        frame.render_widget(
            Table::new(rows, (0..width).map(|_| Constraint::Fill(1)))
                .header(headers)
                .block(
                    Block::new()
                        .fg(Color::LightBlue)
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .title(self.file_names.join(", ")),
                ),
            layout[0],
        );

        // command input
        let line = if self.curr_command.is_empty() {
            Line::from(String::new())
        } else if self.cursor == self.curr_command.len() {
            Line::from(vec![
                Span::from(self.curr_command.clone()),
                Span::from(" ").underlined(),
            ])
        } else {
            let command: Vec<char> = self.curr_command.clone().chars().collect();
            Line::from(vec![
                Span::from(command[..self.cursor].iter().collect::<String>()),
                Span::from(command[self.cursor].to_string()).underlined(),
                Span::from(command[self.cursor + 1..].iter().collect::<String>()),
            ])
        };

        frame.render_widget(
            Paragraph::new(line)
                .style(Style::new().fg(Color::White))
                .block(
                    Block::new()
                        .fg(Color::LightBlue)
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded),
                ),
            layout[1],
        );
    }

    fn get_next_command(&mut self) {
        if let Some(val) = self.command_history.get(self.command_history_idx) {
            self.curr_command = val.clone();
            self.command_history_idx =
                (self.command_history.len() - 1).min(self.command_history_idx + 1);
            self.cursor = self.curr_command.len();
        }
    }

    fn get_prev_command(&mut self) {
        if self.command_history_idx > 0 {
            self.command_history_idx -= 1;
            self.curr_command = self.command_history[self.command_history_idx].clone();
            self.cursor = self.curr_command.len();
        } else {
            self.curr_command = String::new();
            self.cursor = 0;
        }
    }

    const fn move_cursor_right(&mut self) {
        self.cursor = (self.curr_command.len().saturating_sub(1)).min(self.cursor + 1);
    }

    const fn move_cursor_left(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    pub async fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        loop {
            terminal.draw(|frame| self.render(frame))?;
            if let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                match key.code {
                    KeyCode::Esc => return Ok(()),
                    KeyCode::Up => self.get_next_command(),
                    KeyCode::Down => self.get_prev_command(),
                    KeyCode::Right => self.move_cursor_right(),
                    KeyCode::Left => self.move_cursor_left(),
                    KeyCode::PageUp => {
                        self.scroll_offset.0 = self.scroll_offset.0.saturating_sub(3);
                    }
                    KeyCode::PageDown => {
                        self.scroll_offset.0 = self.curr_data.len().min(self.scroll_offset.0 + 3);
                    }
                    KeyCode::Home => self.scroll_offset.1 = self.scroll_offset.1.saturating_sub(1),
                    KeyCode::End => self.scroll_offset.1 += 1,
                    KeyCode::Enter => self.parse_command().await?,
                    KeyCode::Backspace => {
                        if self.cursor != 0 {
                            self.curr_command.remove(self.cursor - 1);
                            self.cursor -= 1;
                        }
                    }
                    _ => {
                        if let Some(k) = key.code.as_char() {
                            self.curr_command.insert(self.cursor, k);
                            self.cursor += 1;
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
        self.cursor = 0;
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
            self.curr_headers = vec![];
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

        if !self.curr_command.trim().is_empty() {
            self.command_history.push_front(self.curr_command.clone());
            self.curr_command = String::new();
        }

        self.cursor = 0;
        self.command_history_idx = 0;
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

        let Ok(file_name) = file_location.clone().into_os_string().into_string() else {
            return Err(anyhow!("could not parse file name into string"));
        };

        let tn = char::from(self.next_table_name);

        self.file_names.push(format!(
            "{} - {}",
            file_location.file_name().unwrap().display(),
            tn
        ));

        build_ctx::add_file_to_ctx(&self.ctx, tn.to_string(), file_name).await?;

        self.send_message(format!("created table {}", tn));
        self.next_table_name += 1;

        Ok(())
    }
}
