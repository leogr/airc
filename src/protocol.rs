//! The bounded UTF-8 IRC profile used by AIRC. Tags have their own size budget.
use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

pub const MAX_LINE: usize = 4608;
pub const MAX_TAGS: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    pub command: String,
    pub params: Vec<String>,
}

impl Message {
    pub fn new(command: &str, params: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            tags: BTreeMap::new(),
            prefix: None,
            command: command.into(),
            params: params.into_iter().map(Into::into).collect(),
        }
    }

    pub fn parse(line: &str) -> Result<Self> {
        let line = line
            .strip_suffix("\r\n")
            .or_else(|| line.strip_suffix('\n'))
            .unwrap_or(line);
        ensure!(!line.contains(['\r', '\n', '\0']), "invalid line delimiter");
        let mut rest = line;
        let mut tags = BTreeMap::new();
        if rest.starts_with('@') {
            let (raw, tail) = rest
                .split_once(' ')
                .ok_or_else(|| anyhow::anyhow!("missing command"))?;
            ensure!(raw.len() <= MAX_TAGS, "tags too long");
            for tag in raw[1..].split(';') {
                let (key, value) = tag.split_once('=').unwrap_or((tag, ""));
                ensure!(
                    !key.is_empty()
                        && key
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b"+-./".contains(&b)),
                    "invalid tag name"
                );
                tags.insert(key.into(), unescape(value));
            }
            rest = tail.trim_start_matches(' ');
        }
        ensure!(rest.len() + 2 <= 512, "IRC message too long");
        let mut prefix = None;
        if let Some(raw) = rest.strip_prefix(':') {
            let (p, tail) = raw
                .split_once(' ')
                .ok_or_else(|| anyhow::anyhow!("missing command"))?;
            ensure!(!p.is_empty(), "empty prefix");
            prefix = Some(p.into());
            rest = tail.trim_start_matches(' ');
        }
        let (command, tail) = rest.split_once(' ').unwrap_or((rest, ""));
        ensure!(
            !command.is_empty()
                && (command.bytes().all(|c| c.is_ascii_alphabetic())
                    || (command.len() == 3 && command.bytes().all(|c| c.is_ascii_digit()))),
            "invalid command"
        );
        let mut params = Vec::new();
        rest = tail.trim_start_matches(' ');
        while !rest.is_empty() {
            if let Some(trailing) = rest.strip_prefix(':') {
                params.push(trailing.into());
                break;
            }
            let (p, tail) = rest.split_once(' ').unwrap_or((rest, ""));
            params.push(p.into());
            rest = tail.trim_start_matches(' ');
        }
        ensure!(params.len() <= 15, "too many parameters");
        Ok(Self {
            tags,
            prefix,
            command: command.to_ascii_uppercase(),
            params,
        })
    }

    pub fn encode(&self) -> Result<String> {
        let mut out = String::new();
        if !self.tags.is_empty() {
            out.push('@');
            for (i, (key, value)) in self.tags.iter().enumerate() {
                if i != 0 {
                    out.push(';');
                }
                out.push_str(key);
                if !value.is_empty() {
                    out.push('=');
                    out.push_str(&escape(value));
                }
            }
            out.push(' ');
        }
        if let Some(prefix) = &self.prefix {
            out.push(':');
            out.push_str(prefix);
            out.push(' ');
        }
        out.push_str(&self.command);
        for (i, param) in self.params.iter().enumerate() {
            ensure!(!param.contains(['\r', '\n', '\0']), "invalid parameter");
            out.push(' ');
            if i == self.params.len() - 1 {
                out.push(':');
            } else {
                ensure!(
                    !param.is_empty() && !param.starts_with(':') && !param.contains(' '),
                    "invalid middle parameter"
                );
            }
            out.push_str(param);
        }
        out.push_str("\r\n");
        let parsed = Self::parse(&out)?;
        ensure!(parsed == *self, "invalid message fields");
        Ok(out)
    }
}

pub fn fold(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '[' | '{' => '{',
            ']' | '}' => '}',
            '\\' | '|' => '|',
            '^' | '~' => '~',
            _ => c.to_ascii_lowercase(),
        })
        .collect()
}

pub fn valid_nick(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 24
        && s.bytes().enumerate().all(|(i, c)| {
            c.is_ascii_alphabetic()
                || b"[]\\`_^{|}".contains(&c)
                || (i > 0 && (c.is_ascii_digit() || c == b'-'))
        })
}

pub fn valid_channel(s: &str) -> bool {
    s.starts_with('#')
        && (2..=48).contains(&s.len())
        && s[1..]
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c))
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace(';', "\\:")
        .replace(' ', "\\s")
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}

fn unescape(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        if let Some(c) = chars.next() {
            out.push(match c {
                ':' => ';',
                's' => ' ',
                'r' => '\r',
                'n' => '\n',
                other => other,
            });
        }
    }
    out
}

/// Does not allocate past the limit, even when a peer never sends a newline.
pub async fn read_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    limit: usize,
) -> Result<Option<String>> {
    let mut out = Vec::new();
    loop {
        let data = reader.fill_buf().await?;
        if data.is_empty() {
            if out.is_empty() {
                return Ok(None);
            }
            bail!("truncated line");
        }
        let count = data
            .iter()
            .position(|b| *b == b'\n')
            .map_or(data.len(), |n| n + 1);
        ensure!(out.len() + count <= limit, "line too long");
        let done = data[count - 1] == b'\n';
        out.extend_from_slice(&data[..count]);
        reader.consume(count);
        if done {
            return Ok(Some(String::from_utf8(out)?));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tags_and_round_trip() {
        let m = Message::parse(
            "@+reply=123;+test=a\\sb\\:c\\\\d\\r\\n :nick!u@h PRIVMSG #c :hello world\r\n",
        )
        .unwrap();
        assert_eq!(m.tags["+test"], "a b;c\\d\r\n");
        assert_eq!(Message::parse(&m.encode().unwrap()).unwrap(), m);
        assert_eq!(fold("N[CK\\^"), "n{ck|~");
    }
    #[test]
    fn rejects_injection_and_oversize() {
        for line in [
            "PRIVMSG #c :hi\r\nOPER root",
            "PRIVMSG #c :\0",
            "@=bad PRIVMSG #c :hi",
        ] {
            assert!(Message::parse(line).is_err());
        }
        assert!(
            Message::new("PRIVMSG", ["#c", &"a".repeat(501)])
                .encode()
                .is_err()
        );
        assert!(!valid_nick("1invalid"));
    }
    #[tokio::test]
    async fn bounded_partial_frames() {
        let mut reader = tokio::io::BufReader::new(&b"PING :x\r\nPART #x\n"[..]);
        assert_eq!(
            read_line(&mut reader, 20).await.unwrap().unwrap(),
            "PING :x\r\n"
        );
        assert_eq!(
            read_line(&mut reader, 20).await.unwrap().unwrap(),
            "PART #x\n"
        );
        assert!(read_line(&mut reader, 20).await.unwrap().is_none());
        assert!(read_line(&mut &b"abcdef"[..], 4).await.is_err());
        assert!(read_line(&mut &b"abc"[..], 4).await.is_err());
    }
}
