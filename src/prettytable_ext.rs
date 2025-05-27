pub fn format_price_level(pl: Option<&[String; 2]>) -> String {
    match pl {
        Some(pl) => format!("{}: {}", pl[0], pl[1]),
        None => "empty".to_string()
    }
}
