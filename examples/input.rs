use crossterm::input::input;
fn read_tx() {
    let input = input();

    match input.read_line() {
        Ok(s) => print!("{}", s),
        Err(e) => println!("error: {}", e),
    }
}

fn main() {
    read_tx();
}
