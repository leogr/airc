//! Client side of IRCv3 draft/multiline: negotiated limits, lossless outbound
//! splitting into batch frames, and bounded assembly of inbound batches.
use crate::protocol::{Message, fold};
use anyhow::{Result, ensure};
use serde::Serialize;

pub const CAP: &str = "draft/multiline";
const CONCAT: &str = "draft/multiline-concat";
/// Largest server limits a client accepts; larger advertisements are not
/// negotiated, so such a server sends ordinary fallback lines instead.
pub const CLIENT_MAX_BYTES: usize = 256 * 1024;
pub const CLIENT_MAX_LINES: usize = 4096;
/// Concurrently open inbound batches before further ones pass through raw.
const MAX_OPEN: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Limits {
    pub max_bytes: usize,
    pub max_lines: usize,
}

impl Limits {
    /// Parse a `draft/multiline` CAP value; `None` if unusable or too large.
    pub fn parse(value: &str) -> Option<Self> {
        let mut max_bytes = None;
        let mut max_lines = None;
        for token in value.split(',') {
            match token.split_once('=') {
                Some(("max-bytes", v)) => max_bytes = v.parse::<usize>().ok(),
                Some(("max-lines", v)) => max_lines = v.parse::<usize>().ok(),
                _ => {}
            }
        }
        let limits = Self {
            max_bytes: max_bytes?,
            max_lines: max_lines.unwrap_or(CLIENT_MAX_LINES),
        };
        (limits.max_bytes >= 512
            && limits.max_bytes <= CLIENT_MAX_BYTES
            && (2..=CLIENT_MAX_LINES).contains(&limits.max_lines))
        .then_some(limits)
    }
}

/// Bytes of message text that fit one relayed line from `nick!account@airc`.
pub fn line_budget(nick: &str, account: &str, command: &str, target: &str) -> usize {
    // ":nick!account@airc COMMAND target :" + text + CRLF, with a safety margin.
    let overhead = 1 + nick.len() + 1 + account.len() + 6 + command.len() + 1 + target.len() + 2;
    512usize.saturating_sub(overhead + 2 + 8).max(16)
}

/// Lines of `text` with concat flags: newlines separate lines and long lines
/// split at UTF-8 boundaries, preferably after a space. Joining is lossless.
pub fn split(text: &str, budget: usize) -> Vec<(String, bool)> {
    let mut lines = Vec::new();
    for logical in text.split('\n') {
        let mut rest = logical;
        let mut concat = false;
        loop {
            let mut cut = rest.len().min(budget);
            while !rest.is_char_boundary(cut) {
                cut -= 1;
            }
            if cut < rest.len()
                && let Some(space) = rest[..cut].rfind(' ')
                && space + 1 > cut / 2
            {
                cut = space + 1;
            }
            lines.push((rest[..cut].to_owned(), concat));
            rest = &rest[cut..];
            concat = true;
            if rest.is_empty() {
                break;
            }
        }
    }
    lines
}

/// Combined text of a composition: lines join with `\n` unless concatenated.
pub fn join(lines: &[(String, bool)]) -> String {
    let mut text = String::new();
    for (n, (line, concat)) in lines.iter().enumerate() {
        if n > 0 && !concat {
            text.push('\n');
        }
        text.push_str(line);
    }
    text
}

/// Byte size counted against `max-bytes`: text plus one byte per newline join.
fn combined(lines: &[(String, bool)]) -> usize {
    join(lines).len()
}

/// Frames for one outbound batch. Client-only tags belong on the opener.
pub fn frames(
    command: &str,
    target: &str,
    reference: &str,
    opener_tags: &std::collections::BTreeMap<String, String>,
    lines: &[(String, bool)],
    limits: Limits,
) -> Result<Vec<Message>> {
    ensure!(
        lines.iter().any(|(line, _)| !line.is_empty()),
        "message has no visible content"
    );
    ensure!(
        combined(lines) <= limits.max_bytes,
        "message is {} bytes; the server accepts {} per message (send larger content as an artifact)",
        combined(lines),
        limits.max_bytes
    );
    ensure!(
        lines.len() <= limits.max_lines,
        "message needs {} lines; the server accepts {} (send larger content as an artifact)",
        lines.len(),
        limits.max_lines
    );
    let mut open = Message::new(
        "BATCH",
        [format!("+{reference}"), CAP.into(), target.into()],
    );
    open.tags = opener_tags.clone();
    let mut out = vec![open];
    for (line, concat) in lines {
        let mut part = Message::new(command, [target, line.as_str()]);
        part.tags.insert("batch".into(), reference.into());
        if *concat {
            part.tags.insert(CONCAT.into(), String::new());
        }
        out.push(part);
    }
    out.push(Message::new("BATCH", [format!("-{reference}")]));
    for frame in &out {
        frame.encode()?;
    }
    Ok(out)
}

/// Wire frames for one outbound PRIVMSG: a single line when it fits, else a
/// batch when negotiated. CR and NUL cannot travel over IRC and are refused.
/// `tags` (client-only metadata) go on the message or the batch opener.
pub fn outbound(
    identity: (&str, &str),
    target: &str,
    text: &str,
    tags: std::collections::BTreeMap<String, String>,
    limits: Option<Limits>,
    reference: &str,
) -> Result<Vec<Message>> {
    ensure!(!text.is_empty(), "message text is empty");
    ensure!(
        !text.contains(['\r', '\0']),
        "message text contains CR or NUL, which IRC cannot carry; use LF line endings"
    );
    let (nick, account) = identity;
    let budget = line_budget(nick, account, "PRIVMSG", target);
    if !text.contains('\n') && (text.len() <= budget || limits.is_none()) {
        let mut message = Message::new("PRIVMSG", [target, text]);
        message.tags = tags;
        message.encode()?;
        return Ok(vec![message]);
    }
    let limits = limits.ok_or_else(|| {
        anyhow::anyhow!("text has newlines but the server did not negotiate draft/multiline")
    })?;
    frames(
        "PRIVMSG",
        target,
        reference,
        &tags,
        &split(text, budget),
        limits,
    )
}

struct Open {
    reference: String,
    opener: Message,
    command: Option<String>,
    lines: Vec<(String, bool)>,
    /// Set once the batch is invalid or over limits; its lines pass through.
    broken: bool,
}

/// Assembles server multiline batches into one logical message each. Invalid
/// or oversized batches degrade to the spec fallback lines (first line carries
/// the opener's tags), never a partial message presented as whole.
pub struct Assembler {
    limits: Limits,
    open: Vec<Open>,
}

impl Assembler {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            open: Vec::new(),
        }
    }

    pub fn push(&mut self, message: Message) -> Vec<Message> {
        if message.command == "BATCH" {
            let first = message.params.first().map(String::as_str).unwrap_or("");
            if let Some(reference) = first.strip_prefix('+')
                && message.params.get(1).map(String::as_str) == Some(CAP)
                && message.params.len() == 3
                && self.open.len() < MAX_OPEN
            {
                self.open.push(Open {
                    reference: reference.to_owned(),
                    opener: message,
                    command: None,
                    lines: Vec::new(),
                    broken: false,
                });
                return Vec::new();
            }
            if let Some(reference) = first.strip_prefix('-')
                && let Some(n) = self.open.iter().position(|o| o.reference == reference)
            {
                let open = self.open.remove(n);
                if open.broken {
                    return Vec::new();
                }
                if open.lines.is_empty() {
                    return Vec::new();
                }
                return vec![assembled(&open)];
            }
            return vec![message];
        }
        let Some(n) = message
            .tags
            .get("batch")
            .and_then(|r| self.open.iter().position(|o| o.reference == *r))
        else {
            return vec![message];
        };
        let limits = self.limits;
        let open = &mut self.open[n];
        if open.broken {
            return vec![fallback(&open.opener, &message, false)];
        }
        let concat = message.tags.contains_key(CONCAT);
        let valid = ["PRIVMSG", "NOTICE"].contains(&message.command.as_str())
            && message.params.len() == 2
            && open.command.as_ref().is_none_or(|c| *c == message.command)
            && fold(&message.params[0]) == fold(&open.opener.params[2])
            && open.lines.len() < limits.max_lines
            && combined(&open.lines)
                + message.params[1].len()
                + usize::from(!open.lines.is_empty() && !concat)
                <= limits.max_bytes;
        if valid {
            open.command = Some(message.command.clone());
            open.lines.push((message.params[1].clone(), concat));
            return Vec::new();
        }
        // Degrade: release what was buffered as fallback lines, then this line.
        open.broken = true;
        let mut out = Vec::new();
        let command = open
            .command
            .clone()
            .unwrap_or_else(|| message.command.clone());
        for (line, _) in std::mem::take(&mut open.lines) {
            let mut m = Message::new(&command, [open.opener.params[2].as_str(), line.as_str()]);
            m.prefix = open.opener.prefix.clone();
            out.push(fallback(&open.opener, &m, out.is_empty()));
        }
        let first = out.is_empty();
        out.push(fallback(&open.opener, &message, first));
        out
    }
}

fn assembled(open: &Open) -> Message {
    let mut tags = open.opener.tags.clone();
    tags.remove(CONCAT);
    Message {
        tags,
        prefix: open.opener.prefix.clone(),
        command: open.command.clone().unwrap_or_else(|| "PRIVMSG".into()),
        params: vec![open.opener.params[2].clone(), join(&open.lines)],
    }
}

/// A component line outside its batch: the opener's parent batch replaces the
/// line's own reference, and the first line also carries the opener's tags.
fn fallback(opener: &Message, line: &Message, first: bool) -> Message {
    let mut m = line.clone();
    m.tags.remove("batch");
    m.tags.remove(CONCAT);
    for (key, value) in &opener.tags {
        if first || ["account", "time", "batch"].contains(&key.as_str()) {
            m.tags.insert(key.clone(), value.clone());
        }
    }
    if m.prefix.is_none() {
        m.prefix = opener.prefix.clone();
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits {
            max_bytes: 1024,
            max_lines: 8,
        }
    }

    fn line(reference: &str, text: &str, concat: bool) -> Message {
        let mut m = Message::new("PRIVMSG", ["#space", text]);
        m.prefix = Some("human!human@airc".into());
        m.tags.insert("batch".into(), reference.into());
        if concat {
            m.tags.insert(CONCAT.into(), String::new());
        }
        m
    }

    fn open(reference: &str, parent: Option<&str>) -> Message {
        let mut m = Message::new(
            "BATCH",
            [format!("+{reference}"), CAP.into(), "#space".into()],
        );
        m.prefix = Some("human!human@airc".into());
        m.tags.insert("msgid".into(), format!("id-{reference}"));
        m.tags.insert("account".into(), "human".into());
        if let Some(parent) = parent {
            m.tags.insert("batch".into(), parent.into());
        }
        m
    }

    #[test]
    fn split_and_join_are_lossless_across_utf8_and_blank_lines() {
        let text = "fn main() {\n\n    println!(\"é漢🙂 x\");\n}\n";
        for budget in [16, 17, 18, 19, 40, 512] {
            let lines = split(text, budget);
            assert_eq!(join(&lines), text, "budget {budget}");
            assert!(lines.iter().all(|(l, _)| l.len() <= budget));
            assert!(lines.iter().all(|(l, concat)| !(*concat && l.is_empty())));
        }
        let long = "word ".repeat(40);
        let lines = split(&long, 50);
        assert!(
            lines[0].0.ends_with(' ') && lines[1].1,
            "breaks after a space"
        );
    }

    #[test]
    fn interleaved_and_nested_batches_assemble_one_message_each() {
        let mut a = Assembler::new(limits());
        assert!(a.push(open("x", Some("h1"))).is_empty());
        assert!(a.push(open("y", None)).is_empty());
        assert!(a.push(line("x", "one", false)).is_empty());
        let unrelated = Message::new("PING", ["t"]);
        assert_eq!(a.push(unrelated.clone()), [unrelated]);
        assert!(a.push(line("y", "solo", false)).is_empty());
        assert!(a.push(line("x", "", false)).is_empty());
        assert!(a.push(line("x", "two ", false)).is_empty());
        assert!(a.push(line("x", "halves", true)).is_empty());
        let y = a.push(Message::new("BATCH", ["-y"]));
        assert_eq!(y[0].params[1], "solo");
        let x = a.push(Message::new("BATCH", ["-x"]));
        assert_eq!(x.len(), 1);
        assert_eq!(x[0].params, ["#space", "one\n\ntwo halves"]);
        assert_eq!(x[0].tags["msgid"], "id-x");
        assert_eq!(x[0].tags["batch"], "h1", "nested history parent kept");
        assert_eq!(x[0].prefix.as_deref(), Some("human!human@airc"));
        // The parent history batch close passes through untouched.
        assert_eq!(a.push(Message::new("BATCH", ["-h1"])).len(), 1);
    }

    #[test]
    fn invalid_or_oversized_batches_degrade_to_fallback_lines() {
        let mut a = Assembler::new(limits());
        a.push(open("z", None));
        a.push(line("z", "first", false));
        let mut wrong = line("z", "other target", false);
        wrong.params[0] = "#other".into();
        let out = a.push(wrong);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].params[1], "first");
        assert_eq!(
            out[0].tags["msgid"], "id-z",
            "first fallback line carries the msgid"
        );
        assert!(!out[1].tags.contains_key("msgid") && !out[1].tags.contains_key("batch"));
        assert_eq!(a.push(line("z", "later", false))[0].params[1], "later");
        assert!(a.push(Message::new("BATCH", ["-z"])).is_empty());
        let mut a = Assembler::new(limits());
        a.push(open("big", None));
        for n in 0..8 {
            assert!(a.push(line("big", &n.to_string(), false)).is_empty());
        }
        let out = a.push(line("big", "ninth", false));
        assert_eq!(
            out.len(),
            9,
            "over max-lines: never a partial whole message"
        );
        assert!(a.push(Message::new("BATCH", ["-big"])).is_empty());
    }

    #[test]
    fn limits_parse_only_bounded_advertisements() {
        let parsed = Limits::parse("max-bytes=65536,max-lines=1024,unknown=1").unwrap();
        assert_eq!((parsed.max_bytes, parsed.max_lines), (65536, 1024));
        assert!(Limits::parse("max-lines=10").is_none());
        assert!(
            Limits::parse("max-bytes=1048576").is_none(),
            "above client cap"
        );
        assert!(Limits::parse("max-bytes=100").is_none());
        let frames = frames(
            "PRIVMSG",
            "#space",
            "s1",
            &Default::default(),
            &split("a\nb", 100),
            parsed,
        )
        .unwrap();
        assert_eq!(frames.len(), 4);
        assert!(
            super::frames(
                "PRIVMSG",
                "#space",
                "s",
                &Default::default(),
                &split("\n", 9),
                parsed
            )
            .is_err()
        );
    }
}
