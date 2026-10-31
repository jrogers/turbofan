//! Rust names for schema names.

const KEYWORDS: &[&str] = &[
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "do", "dyn", "else", "enum",
    "extern", "false", "final", "fn", "for", "gen", "if", "impl", "in", "let", "loop", "macro", "match", "mod", "move",
    "mut", "override", "priv", "pub", "ref", "return", "static", "struct", "trait", "true", "try", "type", "typeof",
    "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

/// Keywords that can't be raw identifiers.
const RESERVED: &[&str] = &["crate", "self", "Self", "super"];

/// Names generated types can't take: they would shadow the prelude, or the types the generated
/// modules import, where the generated code or its users refer to them.
const TYPES_IN_USE: &[&str] = &[
    "AsMut",
    "AsRef",
    "Box",
    "Clone",
    "Code",
    "CompactString",
    "Copy",
    "Decimal",
    "Default",
    "DoubleEndedIterator",
    "Drop",
    "Eq",
    "ExactSizeIterator",
    "Extend",
    "Fn",
    "FnMut",
    "FnOnce",
    "From",
    "FromIterator",
    "Into",
    "IntoIterator",
    "Iterator",
    "MonthYear",
    "NaiveDate",
    "Option",
    "Ord",
    "PartialEq",
    "PartialOrd",
    "Result",
    "Secret",
    "Self",
    "Send",
    "Sized",
    "String",
    "Sync",
    "ToOwned",
    "ToString",
    "TryFrom",
    "TryInto",
    "TzTimeOnly",
    "TzTimestamp",
    "Unpin",
    "UtcTimeOnly",
    "UtcTimestamp",
    "Vec",
];

/// Whether a generated type can't be named `name`.
pub(crate) fn type_in_use(name: &str) -> bool {
    TYPES_IN_USE.contains(&name)
}

/// `ClOrdID` → `cl_ord_id`. A word starts at a capital after a lowercase letter or digit, or at an
/// acronym's last capital when a lowercase letter follows (`MDEntry` → `md_entry`), unless that's a
/// lone `s` pluralising the acronym (`PartyIDs` → `party_ids`). A lowercase `id` ending the name or
/// a word, straight after an acronym of two or more capitals, is a word of its own (`IOIid` →
/// `ioi_id`).
pub(crate) fn snake(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let lower_at = |i: usize| chars.get(i).is_some_and(|c| c.is_ascii_lowercase());
    let upper_at = |i: usize| chars.get(i).is_some_and(|c| c.is_ascii_uppercase());
    let id_at = |i: usize| {
        i >= 2
            && chars.get(i) == Some(&'i')
            && chars.get(i + 1) == Some(&'d')
            && upper_at(i - 1)
            && upper_at(i - 2)
            && chars.get(i + 2).is_none_or(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
    };
    let mut out = String::with_capacity(name.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            let prev = chars[i - 1];
            let plural = chars.get(i + 1) == Some(&'s') && !lower_at(i + 2);
            let word_follows = lower_at(i + 1) && !plural && !id_at(i + 1);
            // An ID ending an acronym is a word of its own: IOIID is IOI ID.
            let trailing_id = c == 'I' && chars.get(i + 1) == Some(&'D') && !lower_at(i + 2);
            if prev.is_ascii_lowercase()
                || prev.is_ascii_digit()
                || (prev.is_ascii_uppercase() && (word_follows || trailing_id))
            {
                out.push('_');
            }
        } else if id_at(i) {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// A usable identifier: keywords become raw identifiers, or get a `_` when they can't be.
pub(crate) fn ident(name: String) -> String {
    if RESERVED.contains(&name.as_str()) {
        name + "_"
    } else if KEYWORDS.contains(&name.as_str()) {
        format!("r#{name}")
    } else {
        name
    }
}

/// The struct field for a schema field.
pub(crate) fn field_ident(name: &str) -> String {
    ident(snake(name))
}

/// The constant for a schema name.
pub(crate) fn constant(name: &str) -> String {
    snake(name).to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_case_splits_words_and_acronyms() {
        assert_eq!(snake("ClOrdID"), "cl_ord_id");
        assert_eq!(snake("MDEntryType"), "md_entry_type");
        assert_eq!(snake("SecurityIDSource"), "security_id_source");
        assert_eq!(snake("IOIID"), "ioi_id");
        assert_eq!(constant("IOIID"), "IOI_ID");
        assert_eq!(snake("Side"), "side");
        assert_eq!(snake("LegPrice2"), "leg_price2");
        assert_eq!(snake("NoPartyIDs"), "no_party_ids");
        assert_eq!(snake("NoNested2PartySubIDs"), "no_nested2_party_sub_ids");
        assert_eq!(snake("IDsSource"), "ids_source");
    }

    #[test]
    fn a_lowercase_id_after_an_acronym_is_its_own_word() {
        assert_eq!(snake("IOIid"), "ioi_id");
        assert_eq!(snake("IOIidSource"), "ioi_id_source");
        assert_eq!(constant("IOIid"), "IOI_ID");
        assert_eq!(snake("MDEntry"), "md_entry");
        assert_eq!(snake("IOIOthSvc"), "ioi_oth_svc");
        assert_eq!(snake("CPRegType"), "cp_reg_type");
        assert_eq!(snake("NoPartyIDs"), "no_party_ids");
        assert_eq!(snake("Valid"), "valid");
        assert_eq!(snake("IDsSource"), "ids_source");
    }

    #[test]
    fn identifiers_avoid_keywords() {
        assert_eq!(field_ident("Yield"), "r#yield");
        assert_eq!(field_ident("Type"), "r#type");
        assert_eq!(field_ident("Self"), "self_");
        assert_eq!(field_ident("ClOrdID"), "cl_ord_id");
    }

    #[test]
    fn constants_are_screaming_snake_case() {
        assert_eq!(constant("ClOrdID"), "CL_ORD_ID");
        assert_eq!(constant("NoAllocs"), "NO_ALLOCS");
        assert_eq!(constant("Yield"), "YIELD");
    }
}
