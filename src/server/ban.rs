//! Bounded RFC1459 nick!account@host masks; no regular expressions or extbans.
use crate::protocol::fold;

pub(super) const MAX_BANS: usize = 100;
const MAX_MASK: usize = 200;

pub(super) fn normalize(raw: &str) -> Option<String> {
    if raw.is_empty()
        || raw.len() > MAX_MASK
        || !raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._[]\\`^{}|~*?!@".contains(&b))
    {
        return None;
    }
    let (nick, tail) = match raw.split_once('!') {
        Some(parts) => parts,
        None if raw.contains('@') => ("*", raw),
        None => (raw, "*"),
    };
    let (user, host) = tail.split_once('@').unwrap_or((tail, "*"));
    if [nick, user, host].iter().any(|s| s.contains(['!', '@'])) {
        return None;
    }
    let part = |s: &str| {
        if s.is_empty() {
            "*".to_owned()
        } else {
            fold(s)
        }
    };
    let mask = format!("{}!{}@{}", part(nick), part(user), part(host));
    (mask.len() <= MAX_MASK).then_some(mask)
}

pub(super) fn matches(mask: &str, prefix: &str) -> bool {
    let folded = fold(prefix);
    let text = folded.as_bytes();
    let mask = mask.as_bytes();
    let (mut m, mut t, mut star, mut retry) = (0, 0, None, 0);
    while t < text.len() {
        if m < mask.len() && (mask[m] == b'?' || mask[m] == text[t]) {
            m += 1;
            t += 1;
        } else if m < mask.len() && mask[m] == b'*' {
            star = Some(m);
            m += 1;
            retry = t;
        } else if let Some(saved) = star {
            retry += 1;
            t = retry;
            m = saved + 1;
        } else {
            return false;
        }
    }
    while m < mask.len() && mask[m] == b'*' {
        m += 1;
    }
    m == mask.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_are_bounded_normalized_and_match_rfc1459_names() {
        for (raw, expected) in [
            ("Alice", "alice!*@*"),
            ("ALICE@airc", "*!alice@airc"),
            ("Nick!User", "nick!user@*"),
            ("Nick!@", "nick!*@*"),
            ("*!*@*", "*!*@*"),
        ] {
            assert_eq!(normalize(raw).as_deref(), Some(expected));
        }
        for raw in [
            "", "a b", "a,b", "a!b!c", "a@b@c", "a@b!c", "$a:alice", "a\x1b", "é",
        ] {
            assert!(normalize(raw).is_none(), "{raw:?}");
        }
        assert!(normalize(&"a".repeat(200)).is_none()); // Expansion also fits the bound.
        assert!(normalize(&"a".repeat(196)).is_some());
        for (mask, prefix, expected) in [
            ("*!alice@*", "DifferentNick!Alice@airc", true),
            ("a?ice!*@airc", "Alice!alice@airc", true),
            ("a?ice!*@airc", "Aice!alice@airc", false),
            ("[nick]!*@*", "{NICK}!bob@airc", true),
            ("*a*b?c!*@*", "aaabzc!alice@airc", true),
            ("*!bob@airc", "bob!alice@airc", false),
        ] {
            assert_eq!(matches(&normalize(mask).unwrap(), prefix), expected);
        }
    }
}
