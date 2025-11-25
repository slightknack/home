//! This module contains markup parsing logic.
//! Markup is very simple for now. We support these items:
//!
//! - Headings, on lines beginning with `#`.
//! - Bullet lists, on lines beginning with `-`.
//! - Normal paragraphs of text.
//!
//! Furthermore, each item may contain bold text.
//! Bold text starts with `*` and ends with `*` or at the end of the line.
//! Any character may be escaped with `\`.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Style {
    Normal,
    Bold,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Frag {
    pub frags: Vec<(Style, String)>,
}

fn process_char(
    c: u8,
    rem: &[u8],
    index: &mut usize,
    buffer: &mut Vec<u8>,
    style: &mut Style,
    frags: &mut Vec<(Style, String)>,
) -> bool {
    match c {
        b'\n' => return true, // Signal to break
        b'\\' => {
            // Escape: skip backslash and append next char literally
            *index += 1;
            if *index < rem.len() {
                buffer.push(rem[*index]);
            }
        }
        b'*' => {
            // Push current buffer if non-empty
            if !buffer.is_empty() {
                let contents = String::from_utf8(buffer.clone()).unwrap();
                frags.push((*style, contents));
                buffer.clear();
            }
            // Toggle style
            *style = match *style {
                Style::Normal => Style::Bold,
                Style::Bold => Style::Normal,
            };
        }
        _ => {
            buffer.push(c);
        }
    }
    return false;
}

/// Parse the next fragment.
/// Filters out empty fragments, like `**` in `race**car`.
/// Does not merge together like fragments after filtering, though!
pub fn parse_frag(rem: &[u8]) -> (Frag, &[u8]) {
    let rem = rem.trim_ascii_start();
    let mut frags = Vec::new();
    let mut style = Style::Normal;
    let mut buffer = Vec::new();
    let mut index = 0;

    while index < rem.len() {
        let c = rem[index];
        if process_char(c, rem, &mut index, &mut buffer, &mut style, &mut frags) {
            break;
        }
        index += 1;
    }

    // Push remaining buffer
    if !buffer.is_empty() {
        let contents = String::from_utf8(buffer).unwrap();
        frags.push((style, contents));
    }

    return (Frag { frags }, &rem[index..]);
}

#[derive(Debug, PartialEq, Eq)]
pub enum Item {
    Heading(Frag),
    Bullet(Frag),
    Line(Frag),
}

/// Parse the next item, if one is available.
pub fn parse_item(rem: &[u8]) -> Option<(Item, &[u8])> {
    let rem = rem.trim_ascii_start();
    if rem.is_empty() { return None; }

    let wrap = |item: fn(Frag) -> Item, (frag, rem)| (item(frag), rem);

    match rem[0] {
        b'#' => Some(wrap(Item::Heading, parse_frag(&rem[1..]))),
        b'-' => Some(wrap(Item::Bullet,  parse_frag(&rem[1..]))),
        _    => Some(wrap(Item::Line,    parse_frag( rem     ))),
    }
}

/// Parse an entire string into a sequence of items.
pub fn parse_string(string: String) -> Vec<Item> {
    let mut items = Vec::new();
    let mut rem = string.as_bytes();

    while let Some((item, r)) = parse_item(rem) {
        items.push(item);
        rem = r;
    }

    return items;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frag_examples() {
        let (f0, r0) = parse_frag(b"normal *bold* normal\nother text");
        assert_eq!(f0.frags, vec![
            (Style::Normal, "normal ".to_string()),
            (Style::Bold, "bold".to_string()),
            (Style::Normal, " normal".to_string()),
        ]);
        assert_eq!(r0, b"\nother text");

        let (f1, r1) = parse_frag(b"empty **\nthis");
        assert_eq!(f1.frags, vec![
            (Style::Normal, "empty ".to_string()),
        ]);
        assert_eq!(r1, b"\nthis");

        let (f2, r2) = parse_frag(b"this is *unclosed bold\nhurrah!");
        assert_eq!(f2.frags, vec![
            (Style::Normal, "this is ".to_string()),
            (Style::Bold, "unclosed bold".to_string()),
        ]);
        assert_eq!(r2, b"\nhurrah!");

        let (f3, r3) = parse_frag(b"this is \\* *escaped*");
        assert_eq!(f3.frags, vec![
            (Style::Normal, "this is * ".to_string()),
            (Style::Bold, "escaped".to_string()),
        ]);
        assert_eq!(r3, b"");
    }

    #[test]
    fn item_examples() {
        let (i0, r0) = parse_item(b"# heading\n").unwrap();
        assert_eq!(i0, Item::Heading(Frag { frags: vec![(Style::Normal, "heading".to_string())] }));
        assert_eq!(r0, b"\n");

        let (i1, r1) = parse_item(b"- bullet\n").unwrap();
        assert_eq!(i1, Item::Bullet(Frag { frags: vec![(Style::Normal, "bullet".to_string())] }));
        assert_eq!(r1, b"\n");

        let (i2, r2) = parse_item(b"line\n").unwrap();
        assert_eq!(i2, Item::Line(Frag { frags: vec![(Style::Normal, "line".to_string())] }));
        assert_eq!(r2, b"\n");
    }

    #[test]
    fn string_example() {
        let s = "# example *new*\n- item 1\n- *item 2*\n- item 3\nthis is some *text* info.";
        let items = parse_string(s.to_string());
        assert_eq!(items, vec![
            Item::Heading(Frag { frags: vec![(Style::Normal, "example ".to_string()), (Style::Bold, "new".to_string())] }),
            Item::Bullet(Frag { frags: vec![(Style::Normal, "item 1".to_string())] }),
            Item::Bullet(Frag { frags: vec![(Style::Bold, "item 2".to_string())] }),
            Item::Bullet(Frag { frags: vec![(Style::Normal, "item 3".to_string())] }),
            Item::Line(Frag { frags: vec![(Style::Normal, "this is some ".to_string()), (Style::Bold, "text".to_string()), (Style::Normal, " info.".to_string())] }),
        ]);
    }
}
