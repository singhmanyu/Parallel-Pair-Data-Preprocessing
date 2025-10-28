import sys
import re
import argparse
import pandas as pd
from unicodedata import category

# Heuristics
ENG_LATIN_RE = re.compile(r"[A-Za-z]")
WHITESPACE_RE = re.compile(r"\s+")
# Characters allowed in Nepali Devanagari range and common punctuation
# Devanagari: \u0900-\u097F; Common punctuation and space
ALLOWED_CHARS_RE = re.compile(r"[\u0900-\u097F\s\.,;:'\-\(\)\[\]\/\?!\u0964\u0965\u0966-\u096F\u200c\u200d]+$")
# Remove zero-width and BOMs
ZERO_WIDTH_RE = re.compile(r"[\u200B\u200C\u200D\uFEFF]")

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
    # Count letters and symbols
    total = len(s)
    letters = sum(1 for ch in s if category(ch).startswith('L'))
    symbols = sum(1 for ch in s if category(ch).startswith('S') or category(ch).startswith('P'))
    # If mostly symbols/punct, flag as nonsense
    if total > 0 and (symbols / total) > max_symbol_ratio:
        return True
    # If too few letters, also suspect
    if total > 0 and (letters / total) < min_alpha_ratio:
        return True
    return False

def main():
    p = argparse.ArgumentParser(description="Preprocess Nepali-English Excel files")
    p.add_argument('--infile', required=True, help='Path to input Excel file')
    p.add_argument('--outfile', required=True, help='Path to output Excel file')
    p.add_argument('--sheet', default=0, help='Sheet name or index (default: 0)')
    p.add_argument('--nepali_col', default='nepali_col', help='Nepali column name (default: nepali_col)')
    p.add_argument('--english_col', default='english_col', help='English column name (default: english_col)')
    p.add_argument('--case_insensitive_dupes', action='store_true', help='Treat duplicates ignoring case')
    p.add_argument('--keep_order', action='store_true', help='Keep first occurrence order when dropping duplicates')
    p.add_argument('--min_alpha_ratio', type=float, default=0.3, help='Minimum letter ratio else considered nonsense (default 0.3)')
    p.add_argument('--max_symbol_ratio', type=float, default=0.5, help='Maximum symbol/punct ratio allowed (default 0.5)')
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

    # Normalize text columns (do not modify other columns)
    df[args.nepali_col] = df[args.nepali_col].map(normalize_text)
    df[args.english_col] = df[args.english_col].map(normalize_text)

    # Drop rows where either side is blank after normalization (treat spaces as blank)
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

    # Filter 3: Remove rows containing disallowed characters (optional but safer)
    # Only apply to Nepali column to avoid removing legitimate English punctuation in English column
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
