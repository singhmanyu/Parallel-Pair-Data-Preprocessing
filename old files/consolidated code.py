import sys
import re
import argparse
import pandas as pd
from unicodedata import category

# Heuristics
ENG_LATIN_RE = re.compile(r"[A-Za-z]")
WHITESPACE_RE = re.compile(r"\s+")

# Allowed chars in Nepali Devanagari range and common punctuation
# Devanagari: \u0900-\u097F; Common punctuation and space
ALLOWED_CHARS_RE = re.compile(r"[\u0900-\u097F\s\.,;:'\-\(\)\[\]\/\?!\u0964\u0965\u0966-\u096F\u200c\u200d]+$")

# Remove zero-width and BOMs
ZERO_WIDTH_RE = re.compile(r"[\u200B\u200C\u200D\uFEFF]")

# Devanagari digit range constants
DEV_ZERO = 0x0966  # U+0966
DEV_NINE = 0x096F  # U+096F

# Regex: strip one leading bullet/number token at start
# Supports:
#  - bullets/dashes: • · ○ ◦ – — -
#  - Arabic digits: 1. 1) (1) (1). 
#  - Alphabet bullets: a) a. A) A.
#  - Roman numerals: i) iv. IX)
#  - Devanagari digits: १. २) (३)
# Anchored to start; consumes following whitespace.
LEADING_BULLET_RE = re.compile(
    r"""^
        \s*                                                     # leading spaces
        (?:
            [\u2022\u00B7\u25CB\u25E6\u2013\u2014\-]            # bullets/dashes: • · ○ ◦ – — -
          | (?:\(\s*\d+\s*\)\.?)                                # (123) or (123).
          | (?:\d+[.)])                                         # 123. or 123)
          | (?:\(\s*[०-९]+\s*\)\.?)                             # (Devanagari digits)
          | (?:[०-९]+[.)])                                      # १२. or १२)
          | (?:[A-Za-z][\).])                                    # a) a. A) A.
          | (?:[IVXLCDMivxlcdm]+[.)])                           # Roman numerals with . or )
        )
        \s*                                                     # trailing spaces after token
    """,
    re.VERBOSE,
)

def strip_one_leading_bullet(s: str) -> str:
    if not isinstance(s, str):
        return s
    s0 = s
    m = LEADING_BULLET_RE.match(s)
    if m:
        return s[m.end():].lstrip()
    return s0

def strip_bullets_repeated(s: str, max_loops: int = 3) -> str:
    if not isinstance(s, str):
        return s
    out = s
    for _ in range(max_loops):
        new = strip_one_leading_bullet(out)
        if new == out:
            break
        out = new
    return out

def to_devanagari_digits_only(s: str) -> str:
    # Map ASCII 0-9 to U+0966..U+096F; leave other chars intact
    if not isinstance(s, str):
        s = str(s) if s is not None else ""
    out_chars = []
    for ch in s:
        if "0" <= ch <= "9":
            out_chars.append(chr(DEV_ZERO + (ord(ch) - ord("0"))))
        else:
            out_chars.append(ch)
    return "".join(out_chars)

def normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = ZERO_WIDTH_RE.sub("", s)
    s = s.replace("\u00A0", " ")
    s = WHITESPACE_RE.sub(" ", s).strip()
    return s

def looks_nonsense(s: str, min_alpha_ratio: float = 0.3, max_symbol_ratio: float = 0.5) -> bool:
    if not s:
        return True
    total = len(s)
    letters = sum(1 for ch in s if category(ch).startswith('L'))
    symbols = sum(1 for ch in s if category(ch).startswith('S') or category(ch).startswith('P'))
    if total > 0 and (symbols / total) > max_symbol_ratio:
        return True
    if total > 0 and (letters / total) < min_alpha_ratio:
        return True
    return False

def main():
    p = argparse.ArgumentParser(description="Preprocess Nepali-English Excel files with bullet cleanup and Devanagari digit conversion (target only)")
    p.add_argument('--infile', required=True, help='Path to input Excel file')
    p.add_argument('--outfile', required=True, help='Path to output Excel file')
    p.add_argument('--sheet', default=0, help='Sheet name or index (default: 0)')
    p.add_argument('--nepali_col', default='nepali_col', help='Nepali column name (default: nepali_col)')
    p.add_argument('--english_col', default='english_col', help='English column name (default: english_col)')
    p.add_argument('--case_insensitive_dupes', action='store_true', help='Treat duplicates ignoring case')
    p.add_argument('--keep_order', action='store_true', help='Keep first occurrence order when dropping duplicates')
    p.add_argument('--min_alpha_ratio', type=float, default=0.3, help='Minimum letter ratio else considered nonsense (default 0.3)')
    p.add_argument('--max_symbol_ratio', type=float, default=0.5, help='Maximum symbol/punct ratio allowed (default 0.5)')
    p.add_argument('--source_first', action='store_true', help='If provided, nepali_col is source and english_col is target; else vice-versa, adjust as needed')
    args = p.parse_args()

    # Read Excel
    df = pd.read_excel(args.infile, sheet_name=args.sheet)

    # Ensure columns exist
    if args.nepali_col not in df.columns:
        raise SystemExit(f"Nepali column '{args.nepali_col}' not found. Available: {list(df.columns)}")
    if args.english_col not in df.columns:
        raise SystemExit(f"English column '{args.english_col}' not found. Available: {list(df.columns)}")

    # Preserve original column order
    cols = list(df.columns)

    # Normalize text columns
    df[args.nepali_col] = df[args.nepali_col].map(normalize_text)
    df[args.english_col] = df[args.english_col].map(normalize_text)

    # Strip leading bullets/numbering in both columns
    df[args.nepali_col] = df[args.nepali_col].map(strip_bullets_repeated)
    df[args.english_col] = df[args.english_col].map(strip_bullets_repeated)

    # Decide which column is target for digit conversion
    # Common workflow: Nepali target → convert ASCII digits to Devanagari in Nepali column
    target_col = args.nepali_col if args.source_first else args.nepali_col
    # If the project uses English→Nepali with Nepali as target, above is correct.
    # If target is English (rare case), set target_col = args.english_col accordingly.

    # Convert only target-side ASCII digits to Devanagari
    df[target_col] = df[target_col].map(to_devanagari_digits_only)

    # Drop rows where either side is blank after normalization
    nonblank_mask = (df[args.nepali_col].astype(str).str.strip() != '') & (df[args.english_col].astype(str).str.strip() != '')
    df = df[nonblank_mask].copy()

    # Filter 1: Remove rows where Nepali column contains any English letters
    mask_eng_in_nepali = df[args.nepali_col].astype(str).str.contains(ENG_LATIN_RE)
    df = df[~mask_eng_in_nepali].copy()

    # Filter 2: Remove rows with nonsense text in either column
    df['__nonsense_nep'] = df[args.nepali_col].map(lambda s: looks_nonsense(s, args.min_alpha_ratio, args.max_symbol_ratio))
    df['__nonsense_eng'] = df[args.english_col].map(lambda s: looks_nonsense(s, args.min_alpha_ratio, args.max_symbol_ratio))
    df = df[~(df['__nonsense_nep'] | df['__nonsense_eng'])].copy()
    df.drop(columns=['__nonsense_nep', '__nonsense_eng'], inplace=True)

    # Filter 3: Disallowed characters (apply to Nepali column)
    allowed_mask = df[args.nepali_col].astype(str).str.fullmatch(ALLOWED_CHARS_RE)
    df = df[allowed_mask].copy()

    # Filter 4: Drop duplicate sentence pairs
    key_nep = df[args.nepali_col].str.lower() if args.case_insensitive_dupes else df[args.nepali_col]
    key_eng = df[args.english_col].str.lower() if args.case_insensitive_dupes else df[args.english_col]
    dedup_key = key_nep.str.cat(key_eng, sep=' ||| ')
    df = df.loc[~dedup_key.duplicated(keep='first' if args.keep_order else False)].copy()

    # Write out preserving column order
    df = df[cols].copy()
    with pd.ExcelWriter(args.outfile, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)

if __name__ == '__main__':
    main()
