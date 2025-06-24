use crate::prelude::handlers::config::CompleteConfig;
use chrono::{DateTime, Utc};
use tui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout},
    terminal::Frame,
    text::Text,
    widgets::{Block, Borders, Paragraph},
};

pub fn get_time() -> String {
    let now_utc: DateTime<Utc> = Utc::now();

    let default_string = now_utc.to_string();
    return format!("Default format: {}", default_string);
}

pub fn draw_ui<T: Backend>(frame: &mut Frame<T>, config: &CompleteConfig) {
    let vertical_chunk_constraints = vec![Constraint::Percentage(50), Constraint::Percentage(50)];

    let margin = config.frontend.margin;
    let default_message = Text::from(String::from(config.frontend.default_message.to_owned()));

    let vertical_chunks = Layout::default()
        .direction(Direction::Vertical)
        .horizontal_margin(margin)
        .vertical_margin(2)
        .constraints(vertical_chunk_constraints.clone())
        .split(frame.size());

    let table =
        Paragraph::new(Text::from(default_message)).block(Block::default().borders(Borders::ALL));

    frame.render_widget(table, vertical_chunks[0]);

    use bitcoin::{Network, Transaction};
    use rust_mempool::MempoolClient;
    let client = MempoolClient::new(Network::Bitcoin);

    let default_message = Text::from(String::from(get_time()));

    let vertical_chunks = Layout::default()
        .direction(Direction::Vertical)
        .horizontal_margin(margin)
        .vertical_margin(2)
        .constraints(vertical_chunk_constraints)
        .split(frame.size());

    let table =
        Paragraph::new(Text::from(default_message)).block(Block::default().borders(Borders::ALL));

    frame.render_widget(table, vertical_chunks[1]);
}
