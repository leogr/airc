//! Shared message references. A canonical UUID msgid is spelled `m:` plus the
//! unpadded base64url encoding of its 16 bytes in display order, 24 ASCII
//! characters in all. The spelling is lossless: it names exactly one server
//! msgid in every client, without local numbering or lookup state. Other msgid
//! forms keep their opaque `msgid:` spelling and never gain an `m:` one; that
//! spelling is offered as a reference only when it names the same bytes again.
use super::state::safe;
use anyhow::{Result, bail, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

const PREFIX: &str = "m:";
/// Encoded characters after the prefix: 128 bits in 22 sextets.
const ENCODED: usize = 22;
/// Bound of an opaque msgid argument, in bytes.
const OPAQUE_BYTES: usize = 128;
/// Characters of an opaque msgid that a record header shows before truncating.
const OPAQUE_CHARS: usize = 128;
const TRUNCATED: &str = "…[truncated]";
/// Marks an opaque ID shown escaped or longer than an argument allows.
const UNUSABLE: &str = "…[unusable]";
/// The header token of an older saved record whose msgid is no longer known.
pub(super) const UNAVAILABLE: &str = "unavailable";

/// The shared reference of a canonical (lowercase, hyphenated) UUID msgid.
pub(super) fn compact(msgid: &str) -> Option<String> {
    let uuid = uuid::Uuid::try_parse(msgid).ok()?;
    (uuid.hyphenated().to_string() == msgid)
        .then(|| format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(uuid.as_bytes())))
}

/// The canonical msgid an `m:` reference names. Anything but exactly 22
/// base64url characters whose decoding re-encodes to the same text is refused:
/// padding, whitespace, other alphabets, other lengths and nonzero unused bits.
pub(super) fn decode(reference: &str) -> Result<String> {
    let invalid = || anyhow::anyhow!("invalid m: reference");
    let encoded = reference.strip_prefix(PREFIX).ok_or_else(invalid)?;
    if encoded.len() != ENCODED
        || !encoded
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(invalid());
    }
    let bytes: [u8; 16] = URL_SAFE_NO_PAD
        .decode(encoded)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(invalid)?;
    if URL_SAFE_NO_PAD.encode(bytes) != encoded {
        return Err(invalid());
    }
    Ok(uuid::Uuid::from_bytes(bytes).hyphenated().to_string())
}

/// The exact msgid a `show` or `--reply` argument names: an `m:` reference,
/// `msgid:ID` or a raw msgid. Local `@N` numbers are not references.
pub(super) fn resolve(argument: &str) -> Result<String> {
    if argument.starts_with('@') {
        bail!(
            "local @N numbers are not message references; use the message's m: reference or its msgid"
        );
    }
    if argument.starts_with(PREFIX) {
        return decode(argument);
    }
    let msgid = argument.strip_prefix("msgid:").unwrap_or(argument);
    ensure!(
        !msgid.ends_with(TRUNCATED) && !msgid.ends_with(UNUSABLE),
        "the ID is cut or escaped in the record and cannot be used"
    );
    ensure!(
        !msgid.is_empty()
            && msgid.len() <= OPAQUE_BYTES
            && !msgid.chars().any(|c| c.is_whitespace() || c.is_control()),
        "invalid message reference; use an m: reference or a msgid"
    );
    Ok(msgid.into())
}

/// How a record names a msgid: [`exact`] when it has that, else `msgid:` and
/// the bounded, escaped opaque ID, marked truncated or unusable, so the header
/// stays one word and no argument can take it.
pub(super) fn present(msgid: &str) -> String {
    exact(msgid).unwrap_or_else(|| {
        let bounded: String = msgid.chars().take(OPAQUE_CHARS).collect();
        let marker = if bounded.len() < msgid.len() {
            TRUNCATED
        } else {
            UNUSABLE
        };
        format!("msgid:{}{marker}", safe(&bounded).replace(' ', "\\u{20}"))
    })
}

/// The spelling that names exactly this msgid when given back to [`resolve`]:
/// its `m:` reference, or `msgid:` and the opaque ID unchanged. None when
/// display would escape the ID or an argument could not carry it.
pub(super) fn exact(msgid: &str) -> Option<String> {
    compact(msgid).or_else(|| {
        let token = format!("msgid:{msgid}");
        (safe(msgid) == msgid && resolve(&token).is_ok_and(|id| id == msgid)).then_some(token)
    })
}

/// Whether a header token is a shared reference that `show` and `--reply`
/// take back to the identical msgid.
pub(super) fn shareable(token: &str) -> bool {
    resolve(token).is_ok_and(|id| exact(&id).as_deref() == Some(token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct Valid {
        msgid: String,
        reference: String,
    }

    #[derive(serde::Deserialize)]
    struct Opaque {
        msgid: String,
        presented: String,
        shareable: bool,
    }

    #[derive(serde::Deserialize)]
    struct Vectors {
        valid: Vec<Valid>,
        invalid: Vec<String>,
        uncompactable: Vec<String>,
        opaque: Vec<Opaque>,
    }

    #[test]
    fn shared_vectors_round_trip_exactly_and_refuse_every_other_spelling() {
        let vectors: Vectors =
            serde_json::from_str(include_str!("../../tests/data/message-references.json")).unwrap();
        for v in &vectors.valid {
            assert_eq!(compact(&v.msgid).as_deref(), Some(v.reference.as_str()));
            assert_eq!(decode(&v.reference).unwrap(), v.msgid);
            assert_eq!(resolve(&v.reference).unwrap(), v.msgid);
            assert_eq!(present(&v.msgid), v.reference);
            assert!(shareable(&v.reference));
        }
        for reference in &vectors.invalid {
            assert!(decode(reference).is_err(), "{reference:?}");
        }
        for msgid in &vectors.uncompactable {
            assert_eq!(compact(msgid), None, "{msgid:?}");
        }
        for o in &vectors.opaque {
            assert_eq!(present(&o.msgid), o.presented, "{:?}", o.msgid);
            assert_eq!(shareable(&o.presented), o.shareable, "{:?}", o.msgid);
            match resolve(&o.presented) {
                Ok(id) if o.shareable => assert_eq!(id, o.msgid),
                Ok(id) => assert_ne!(id, o.msgid, "an unusable spelling must not name the ID"),
                Err(_) => assert!(!o.shareable),
            }
        }
    }

    #[test]
    fn every_unused_bit_pattern_of_the_final_character_is_refused() {
        let msgid = "123e4567-e89b-42d3-a456-426614174000";
        let reference = compact(msgid).unwrap();
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut accepted = Vec::new();
        for &last in alphabet {
            let mut alias = reference[..reference.len() - 1].to_string();
            alias.push(last as char);
            if decode(&alias).is_ok() {
                accepted.push(last as char);
            }
        }
        // Only the two data bits vary; each pattern names a different msgid.
        assert_eq!(accepted, ['A', 'Q', 'g', 'w']);
    }

    #[test]
    fn arguments_take_shared_and_full_forms_and_refuse_local_numbers() {
        let msgid = "123e4567-e89b-42d3-a456-426614174000";
        for argument in [
            "m:Ej5FZ-ibQtOkVkJmFBdAAA",
            "msgid:123e4567-e89b-42d3-a456-426614174000",
            msgid,
        ] {
            assert_eq!(resolve(argument).unwrap(), msgid);
        }
        assert_eq!(resolve("msgid:opaque-1").unwrap(), "opaque-1");
        assert_eq!(resolve("opaque-1").unwrap(), "opaque-1");
        let local = resolve("@12").unwrap_err().to_string();
        assert!(local.contains("not message references") && local.contains("m: reference"));
        assert!(resolve("@").is_err());
        // A malformed reserved form never falls through as an opaque msgid.
        assert!(resolve("m:Ej5FZ-ibQtOkVkJmFBdAAB").is_err());
        assert!(resolve("m:short").is_err());
        let long = "x".repeat(129);
        for invalid in ["", "msgid:", "a b", "id\u{7}", long.as_str()] {
            assert!(resolve(invalid).is_err(), "{invalid:?}");
        }
        for marker in [TRUNCATED, UNUSABLE] {
            assert!(
                resolve(&format!("msgid:{}{marker}", "x".repeat(100)))
                    .unwrap_err()
                    .to_string()
                    .contains("cut or escaped")
            );
        }
    }

    #[test]
    fn opaque_ids_keep_a_bounded_spelling_and_only_whole_ones_are_exact() {
        assert_eq!(present("id-1"), "msgid:id-1");
        assert_eq!(exact("id-1").as_deref(), Some("msgid:id-1"));
        assert_eq!(present("@1"), "msgid:@1");
        let long = "x".repeat(129);
        assert_eq!(
            present(&long),
            format!("msgid:{}{TRUNCATED}", "x".repeat(128))
        );
        assert_eq!(exact(&long), None);
        assert!(!shareable(&present(&long)));
        // Escaped, spaced or byte-long IDs keep a one-word display, never
        // offered as a reference: show and --reply would name other bytes.
        assert_eq!(
            present("id\nforged"),
            format!("msgid:id\\nforged{UNUSABLE}")
        );
        assert_eq!(present("a\\b"), format!("msgid:a\\\\b{UNUSABLE}"));
        assert_eq!(present("a b"), format!("msgid:a\\u{{20}}b{UNUSABLE}"));
        let wide = "é".repeat(128);
        assert_eq!(present(&wide), format!("msgid:{wide}{UNUSABLE}"));
        for id in ["id\nforged", "a\\b", "a b", wide.as_str()] {
            assert_eq!(exact(id), None, "{id:?}");
            assert!(!shareable(&present(id)), "{id:?}");
        }
        // An escaped spelling an earlier release saved is not shareable either.
        assert!(!shareable("msgid:a\\\\b"));
        let upper = "123E4567-E89B-42D3-A456-426614174000";
        assert_eq!(present(upper), format!("msgid:{upper}"));
        assert!(shareable(&present(upper)));
        assert!(shareable("msgid:id-1") && !shareable(UNAVAILABLE) && !shareable("@1"));
        assert!(!shareable("live-only") && !shareable("id-1") && !shareable("msgid:"));
    }
}
