//! Staged attachments: one logical message carrying several artifacts.
//!
//! A client first stages each file: the server stores the bytes for that
//! account and channel without announcing them, and nobody can download them.
//! The client then sends one PRIVMSG whose text holds an `[Image #k]` marker for
//! every attachment, listing each staged ID with its marker's byte offset in the
//! client-only `+airc-attach` tag. The server verifies ownership and positions,
//! strips that tag and publishes the text, all artifacts and the reserved
//! `airc-artifacts` tag in one store transaction.

use serde::{Deserialize, Serialize};

pub const CLIENT_TAG: &str = "+airc-attach";
pub const RESERVED_TAG: &str = "airc-artifacts";
pub const MAX_ATTACHMENTS: usize = 8;
/// Live staged artifacts one account may hold across all channels; their
/// bytes are bounded by this count times the configured `max_bytes`.
pub const STAGED_PER_ACCOUNT: usize = 8;
/// Bytes of all stored staged rows, including expired rows not yet swept.
pub const STAGED_TOTAL_BYTES: usize = 64 << 20;
/// Seconds a staged artifact stays publishable.
pub const STAGE_TTL_SECONDS: i64 = 600;
/// Expired rows deleted per sweep, so one request does bounded work.
pub const SWEEP_LIMIT: usize = 64;
/// Record headers stay below Claude Code's monitor line cut (~500 characters)
/// and the plugin's `--wrap 480`, so a header is never split into `| ` lines.
pub const HEADER_BUDGET: usize = 400;

/// The marker for the k-th (1-based) attachment.
pub fn marker(k: usize) -> String {
    format!("[Image #{k}]")
}

/// One staged artifact referenced by a send, at its marker's byte offset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attach {
    pub id: String,
    pub at: usize,
}

/// A published attachment, as carried by the reserved tag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Published {
    pub id: String,
    pub bytes: u64,
    pub sha256: String,
    pub at: usize,
    pub name: String,
}

/// Canonical lowercase UUID, as the server generates artifact IDs.
pub fn valid_id(id: &str) -> bool {
    id.len() == 36 && uuid::Uuid::parse_str(id).is_ok_and(|u| u.hyphenated().to_string() == id)
}

pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 100
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

fn valid_sha256(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// A decimal without sign, spaces or leading zeros.
fn number(raw: &str) -> Option<u64> {
    let n = raw.parse::<u64>().ok()?;
    (n.to_string() == raw).then_some(n)
}

/// A byte offset within the largest logical message.
fn offset(raw: &str) -> Option<usize> {
    usize::try_from(number(raw)?)
        .ok()
        .filter(|at| *at <= crate::multiline::CLIENT_MAX_BYTES)
}

/// A complete, unique list of 1–8 references in a valid grammar.
pub fn valid_refs(refs: &[Attach]) -> bool {
    (1..=MAX_ATTACHMENTS).contains(&refs.len())
        && refs.iter().all(|r| valid_id(&r.id))
        && refs
            .iter()
            .enumerate()
            .all(|(i, r)| refs[..i].iter().all(|o| o.id != r.id))
}

pub fn encode_refs(refs: &[Attach]) -> String {
    refs.iter()
        .map(|r| format!("{}@{}", r.id, r.at))
        .collect::<Vec<_>>()
        .join(",")
}

pub fn parse_refs(raw: &str) -> Option<Vec<Attach>> {
    let refs = raw
        .split(',')
        .map(|item| {
            let (id, at) = item.split_once('@')?;
            Some(Attach {
                id: id.into(),
                at: offset(at)?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    valid_refs(&refs).then_some(refs)
}

/// Every marker sits at its offset, in order, without overlap. Offsets are
/// bytes of the logical text, so they are UTF-8 boundaries by construction.
pub fn valid_positions(text: &str, refs: &[Attach]) -> bool {
    let mut free = 0;
    for (i, r) in refs.iter().enumerate() {
        let marker = marker(i + 1);
        // Range checks come first; the addition below cannot overflow.
        if r.at < free
            || r.at > text.len()
            || !text.is_char_boundary(r.at)
            || !text[r.at..].starts_with(&marker)
        {
            return false;
        }
        free = r.at + marker.len();
    }
    true
}

pub fn encode_published(items: &[Published]) -> String {
    items
        .iter()
        .map(|p| format!("{}/{}/{}/{}/{}", p.id, p.bytes, p.sha256, p.at, p.name))
        .collect::<Vec<_>>()
        .join(",")
}

/// The reserved tag, accepted only when every entry is valid.
pub fn parse_published(raw: &str) -> Option<Vec<Published>> {
    let items = raw
        .split(',')
        .map(|item| {
            let mut parts = item.splitn(5, '/');
            let item = Published {
                id: parts.next()?.into(),
                bytes: number(parts.next()?)?,
                sha256: parts.next()?.into(),
                at: offset(parts.next()?)?,
                name: parts.next()?.into(),
            };
            (valid_id(&item.id) && valid_sha256(&item.sha256) && valid_name(&item.name))
                .then_some(item)
        })
        .collect::<Option<Vec<_>>>()?;
    let refs: Vec<_> = items
        .iter()
        .map(|p| Attach {
            id: p.id.clone(),
            at: p.at,
        })
        .collect();
    valid_refs(&refs).then_some(items)
}

/// The compact record summary ` attachments=N:k@OFFSET/UUID/BYTES,…`. It lists
/// items while the whole header stays within `budget` bytes and ends with
/// `,+R` (or `+R`) for R unlisted items, so it never implies a complete list.
pub fn summary(items: &[Published], used: usize, budget: usize) -> String {
    let mut out = format!(" attachments={}:", items.len());
    for (i, p) in items.iter().enumerate() {
        let item = format!("{}@{}/{}/{}", i + 1, p.at, p.id, p.bytes);
        let rest = items.len() - i - 1;
        // Room for this item plus, if any remain, the longest omission suffix.
        let tail = if rest == 0 { 0 } else { 3 };
        let sep = usize::from(i > 0);
        if used + out.len() + sep + item.len() + tail > budget {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&format!("+{}", items.len() - i));
            return out;
        }
        if i > 0 {
            out.push(',');
        }
        out.push_str(&item);
    }
    out
}

/// Full authoritative metadata, one line per attachment, for `show`. Lines
/// start with `!`, which a record body (always `| `-prefixed) cannot forge.
pub fn detail(items: &[Published]) -> String {
    items
        .iter()
        .enumerate()
        .map(|(i, p)| {
            format!(
                "! attachment {} artifact={} name={} bytes={} sha256={} at={}",
                i + 1,
                p.id,
                p.name,
                p.bytes,
                p.sha256,
                p.at
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "0f8fad5b-d9cb-469f-a165-70867728950e";
    const B: &str = "7c9e6679-7425-40de-944b-e07fc1f90ae7";

    fn item(id: &str, at: usize, name: &str) -> Published {
        Published {
            id: id.into(),
            bytes: 68397,
            sha256: "a".repeat(64),
            at,
            name: name.into(),
        }
    }

    #[test]
    fn references_round_trip_and_reject_noncanonical_forms() {
        let refs = vec![
            Attach {
                id: A.into(),
                at: 0,
            },
            Attach {
                id: B.into(),
                at: 17,
            },
        ];
        assert_eq!(parse_refs(&encode_refs(&refs)), Some(refs));
        for bad in [
            "",
            &format!("{A}@01"),
            &format!("{A}@-1"),
            &format!("{A}@ 1"),
            &format!("{}@1", A.to_uppercase()),
            &format!("{}@1", &A[..35]),
            &format!("{A}@1,{A}@20"),
            &format!("{A}1"),
            &vec![format!("{A}@0"); 9].join(","),
        ] {
            assert_eq!(parse_refs(bad), None, "{bad}");
        }
    }

    #[test]
    fn positions_must_hold_their_ordered_markers_without_overlap() {
        let text = "look [Image #1]\nand [Image #2] é";
        let at = |s: &str| text.find(s).unwrap();
        let refs = |a: usize, b: usize| {
            vec![
                Attach {
                    id: A.into(),
                    at: a,
                },
                Attach {
                    id: B.into(),
                    at: b,
                },
            ]
        };
        assert!(valid_positions(
            text,
            &refs(at("[Image #1]"), at("[Image #2]"))
        ));
        // Swapped markers, a literal elsewhere, overlap and a cut character.
        assert!(!valid_positions(
            text,
            &refs(at("[Image #2]"), at("[Image #1]"))
        ));
        assert!(!valid_positions("[Image #1][Image #1]", &refs(0, 10)));
        assert!(!valid_positions("[Image #1]", &refs(0, 2)));
        assert!(!valid_positions(
            text,
            &refs(at("[Image #1]"), text.len() - 1)
        ));
        assert!(!valid_positions(
            text,
            &refs(at("[Image #1]"), text.len() + 5)
        ));
    }

    #[test]
    fn huge_offsets_are_refused_without_overflow() {
        for at in [
            u64::MAX.to_string(),
            usize::MAX.to_string(),
            (crate::multiline::CLIENT_MAX_BYTES + 1).to_string(),
        ] {
            assert_eq!(parse_refs(&format!("{A}@{at}")), None, "{at}");
            let item = format!("{A}/1/{}/{at}/a.png", "a".repeat(64));
            assert_eq!(parse_published(&item), None, "{at}");
        }
        // An offset that bypassed parsing is refused before any arithmetic.
        for at in [usize::MAX, usize::MAX - 9, 11] {
            let refs = [
                Attach {
                    id: A.into(),
                    at: 0,
                },
                Attach { id: B.into(), at },
            ];
            assert!(!valid_positions("[Image #1]", &refs), "{at}");
        }
    }

    #[test]
    fn published_tag_is_strict_and_unique() {
        let items = vec![item(A, 3, "paste-1.png"), item(B, 20, "paste-1.png")];
        let raw = encode_published(&items);
        assert_eq!(parse_published(&raw), Some(items));
        assert_eq!(parse_published(&raw.replace("paste-1.png", "a/b")), None);
        assert_eq!(parse_published(&raw.replace(B, A)), None);
        assert_eq!(parse_published(&raw.replace("68397", "068397")), None);
        assert_eq!(parse_published(&format!("{raw},")), None);
    }

    #[test]
    fn summary_stays_within_its_budget_and_counts_what_it_omits() {
        let items: Vec<_> = (0..8)
            .map(|i| item(if i % 2 == 0 { A } else { B }, i * 20, &"n".repeat(100)))
            .collect();
        let full = summary(&items[..1], 0, HEADER_BUDGET);
        assert_eq!(full, format!(" attachments=1:1@0/{A}/68397"));
        for used in [0, 60, 200, 380, 400] {
            let s = summary(&items, used, HEADER_BUDGET);
            assert!(s.starts_with(" attachments=8:"), "{s}");
            let listed = s.matches('@').count();
            if listed < 8 {
                assert!(s.ends_with(&format!("+{}", 8 - listed)), "{s}");
            }
            if used + s.len() > HEADER_BUDGET {
                assert_eq!(listed, 0, "only the bare count may exceed the budget: {s}");
            }
        }
    }

    #[test]
    fn detail_lines_carry_every_field_and_cannot_look_like_body_lines() {
        let d = detail(&[item(A, 3, "paste-1.png")]);
        assert_eq!(
            d,
            format!(
                "! attachment 1 artifact={A} name=paste-1.png bytes=68397 sha256={} at=3",
                "a".repeat(64)
            )
        );
        assert!(!d.starts_with("| "));
    }
}
