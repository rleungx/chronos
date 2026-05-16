use std::error::Error;

type ParseResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub(crate) fn parse_csv_string_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

pub(crate) fn parse_boolish(value: &str) -> ParseResult<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(format!("invalid boolean value: {other}").into()),
    }
}

pub(crate) fn parse_csv_u32_list(value: &str) -> ParseResult<Vec<u32>> {
    let mut parsed = Vec::new();
    for token in value.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        parsed.push(
            trimmed
                .parse::<u32>()
                .map_err(|error| format!("invalid u32 value {trimmed}: {error}"))?,
        );
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    #[test]
    fn csv_u32_parser_skips_empty_entries() {
        assert_eq!(super::parse_csv_u32_list("1, 2,,3").unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn boolish_parser_accepts_operator_values() {
        assert!(super::parse_boolish("yes").unwrap());
        assert!(!super::parse_boolish("off").unwrap());
        assert!(super::parse_boolish("maybe").is_err());
    }
}
