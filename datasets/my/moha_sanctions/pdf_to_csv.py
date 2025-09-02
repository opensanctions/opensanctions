import logging
import os
import re
import traceback
import unicodedata
from datetime import datetime

import pandas as pd
import pdfplumber

# Malay months (lowercased)
MALAY_MONTHS = {
    'januari': 1, 'februari': 2, 'mac': 3, 'april': 4, 'mei': 5, 'jun': 6,
    'julai': 7, 'ogos': 8, 'september': 9, 'oktober': 10, 'november': 11, 'disember': 12,
}
# English month names/abbrevs (so %B/%b style also works regardless of locale)
EN_MONTHS = {
    'january': 1, 'jan': 1,
    'february': 2, 'feb': 2,
    'march': 3, 'mar': 3,
    'april': 4, 'apr': 4,
    'may': 5,
    'june': 6, 'jun': 6,
    'july': 7, 'jul': 7,
    'august': 8, 'aug': 8,
    'september': 9, 'sep': 9, 'sept': 9,
    'october': 10, 'oct': 10,
    'november': 11, 'nov': 11,
    'december': 12, 'dec': 12,
}

PLACEHOLDERS = {'', '-', '–', '—'}

def _clean(s: str) -> str:
    s = unicodedata.normalize('NFKC', str(s)).strip()
    s = re.sub(r'\s+', ' ', s)
    return s

def _month_from_name(name: str):
    n = name.lower()
    return MALAY_MONTHS.get(n, EN_MONTHS.get(n))

def convert_date_format(date_str: str) -> str:
    """
    Convert any dates in `date_str` and return a single space-separated string:
      - %Y-%m-%d if day+month+year present
      - %Y-%m     if month+year only
      - %Y        if year only
    Malay month names are supported. Returns '' for placeholders or no match.
    """
    if date_str is None:
        return ''
    raw = str(date_str).strip()
    if raw in PLACEHOLDERS:
        return ''
    s = _clean(raw)

    # collect non-overlapping matches (higher priority wins on overlap)
    # priority: 3 = full date; 2 = month-year; 1 = year
    tokens = []  # (start, end, priority, formatted)

    def add_token(start: int, end: int, priority: int, text: str):
        keep = True
        to_remove = []
        for i, (a, b, p, _) in enumerate(tokens):
            overlap = not (end <= a or start >= b)
            if overlap:
                if priority > p:
                    to_remove.append(i)
                else:
                    keep = False
                    break
        if keep:
            for i in reversed(to_remove):
                tokens.pop(i)
            tokens.append((start, end, priority, text))

    # 1) 'D Month Y' (Malay/English), e.g. "12 Disember 2014", "12 Nov 2014"
    for m in re.finditer(r'\b(\d{1,2})\s+([A-Za-zÀ-ÖØ-öø-ÿ]+)\s+(\d{4})\b', s):
        d_s, mon_s, y_s = m.groups()
        mon = _month_from_name(mon_s)
        if mon:
            d, y = int(d_s), int(y_s)
            try:
                add_token(m.start(), m.end(), 3, datetime(y, mon, d).strftime('%Y-%m-%d'))
            except ValueError:
                pass

    # 2) 'Month Y' (no day), e.g. "Disember 2014", "Nov 2014"
    for m in re.finditer(r'\b([A-Za-zÀ-ÖØ-öø-ÿ]+)\s+(\d{4})\b', s):
        mon_s, y_s = m.groups()
        mon = _month_from_name(mon_s)
        if mon:
            y = int(y_s)
            add_token(m.start(), m.end(), 2, f'{y:04d}-{mon:02d}')

    # 3) D[./-]M[./-]Y  (e.g., 9.12.1961, 9/12/1961, 9-12-1961)
    for m in re.finditer(r'\b(\d{1,2})[./-](\d{1,2})[./-](\d{4})\b', s):
        d, mo, y = map(int, m.groups())
        try:
            add_token(m.start(), m.end(), 3, datetime(y, mo, d).strftime('%Y-%m-%d'))
        except ValueError:
            pass

    # 4) Y[./-]M[./-]D (ISO-ish), e.g., 2014-11-12
    for m in re.finditer(r'\b(\d{4})[./-](\d{1,2})[./-](\d{1,2})\b', s):
        y, mo, d = map(int, m.groups())
        try:
            add_token(m.start(), m.end(), 3, datetime(y, mo, d).strftime('%Y-%m-%d'))
        except ValueError:
            pass

    # 5) M[./-]Y (no day), e.g., 02/1976, 2-1976, 2.1976  -> %Y-%m
    for m in re.finditer(r'\b(\d{1,2})[./-](\d{4})\b', s):
        mo, y = map(int, m.groups())
        # avoid overlaps with already captured full dates
        overlaps_full = any(not (m.end() <= a or m.start() >= b) for (a, b, p, _) in tokens if p == 3)
        if overlaps_full:
            continue
        if 1 <= mo <= 12:
            add_token(m.start(), m.end(), 2, f'{y:04d}-{mo:02d}')

    # 6) Year only (1900–2099) -> %Y
    for m in re.finditer(r'\b(19\d{2}|20\d{2})\b', s):
        y = int(m.group(1))
        # avoid overlaps with Month-Year or Full dates
        overlaps = any(not (m.end() <= a or m.start() >= b) for (a, b, _, _) in tokens)
        if overlaps:
            continue
        add_token(m.start(), m.end(), 1, f'{y:04d}')

    if not tokens:
        return ''

    tokens.sort(key=lambda t: t[0])  # by appearance
    return ' '.join(text for (_, _, _, text) in tokens)

def clean_text(text):
    """
    Clean and normalize text by removing extra whitespaces and handling None values.
    
    Args:
        text (str or None): Input text to clean
    
    Returns:
        str: Cleaned text or empty string
    """
    if text is None:
        return ''
    return ' '.join(str(text).split())

def is_header_row(row, expected_columns):
    """
    Check if a row is a header row by comparing its content to expected column names.
    
    Args:
        row (list): A row of table data
        expected_columns (list): List of expected column names
    
    Returns:
        bool: True if the row appears to be a header, False otherwise
    """
    # Clean the row
    cleaned_row = [clean_text(cell) for cell in row]
    
    # Completely skip rows that don't match the expected number of columns
    if len(cleaned_row) != len(expected_columns):
        return True
    
    # Specific column name fragments to check
    column_fragments = {
        'INDIVIDU': [
            'no', 'rujukan', 'nama', 'gelaran', 'jawatan', 
            'tarikh', 'lahir', 'tempat', 'warganegara', 
            'nombor', 'pasport', 'alamat', 'disenaraikan'
        ],
        'KUMPULAN': [
            'no', 'ruj', 'nama', 'alias', 
            'lain', 'alamat', 'disenaraikan'
        ]
    }
    
    # Determine which set of fragments to use based on the number of columns
    fragments = (column_fragments['INDIVIDU'] if len(expected_columns) == 13 
                 else column_fragments['KUMPULAN'])
    
    # Count how many column fragments are present
    fragment_matches = sum(
        any(frag in clean_text(cell).lower() for frag in fragments) 
        for cell in cleaned_row
    )
    
    # Check for numeric or reference-like first column
    first_column_valid = (
        re.match(r'^\d+$', cleaned_row[0]) or 
        re.match(r'^KDN\.\w+\-\d+$', cleaned_row[0]) or 
        cleaned_row[0].lower() in ['no', 'no.']
    )
    
    # Consider it a header if:
    # 1. More than a third of cells contain column fragments, OR
    # 2. The first column is not a valid numeric or reference value
    return (fragment_matches > len(cleaned_row) // 3) or not first_column_valid

def extract_pdf_tables(pdf_path):
    """
    Extract tables from a PDF file and convert them to CSV.
    
    Args:
        pdf_path (str): Path to the PDF file
    
    Returns:
        dict: A dictionary of DataFrames, with section names as keys
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s: %(message)s')
    logger = logging.getLogger(__name__)
    
    # Validate PDF path
    if not os.path.exists(pdf_path):
        logger.error(f"PDF file not found: {pdf_path}")
        return {}
    
    extracted_tables = {}
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Sections to extract
            sections = {
                'INDIVIDU': {
                    'columns': [
                        'No.', 'Rujukan', 'Nama', 'Gelaran', 'Jawatan', 
                        'Tarikh Lahir', 'Tempat Lahir', 'Nama Lain', 
                        'Warganegara', 'Nombor Pasport', 
                        'Nombor. Kad Pengenalan', 'Alamat', 
                        'Tarikh Disenaraikan'
                    ],
                    'data': []
                },
                'KUMPULAN': {
                    'columns': [
                        'No.', 'No. Ruj.', 'Nama', 'Alias', 
                        'Nama Lain', 'Alamat', 'Tarikh Disenaraikan'
                    ],
                    'data': []
                }
            }
            
            current_section = None
            
            for page in pdf.pages:
                # Extract text to help identify sections
                text = page.extract_text()
                
                # Identify section
                if 'A. INDIVIDU' in text:
                    current_section = 'INDIVIDU'
                elif 'B. KUMPULAN' in text:
                    current_section = 'KUMPULAN'
                
                # Extract tables
                tables = page.extract_tables()
                
                for table in tables:
                    # Find the first non-header row
                    start_row = 0
                    for i, row in enumerate(table):
                        # Skip rows that are clearly headers or don't match column count
                        if not is_header_row(row, sections[current_section]['columns']):
                            start_row = i
                            break
                    
                    # Process ALL rows from the first non-header row
                    for row in table[start_row:]:
                        # Clean each cell in the row
                        cleaned_row = [clean_text(cell) for cell in row]
                        
                        # Add ALL non-empty rows with the correct number of columns
                        if (any(cleaned_row) and 
                            len(cleaned_row) == len(sections[current_section]['columns']) and
                            re.match(r'^\d+$', cleaned_row[0])):
                            if current_section:
                                # Convert date columns to dd/mm/yyyy format
                                if current_section == 'INDIVIDU':
                                    # Convert Tarikh Lahir and Tarikh Disenaraikan
                                    cleaned_row[5] = convert_date_format(cleaned_row[5])
                                    cleaned_row[12] = convert_date_format(cleaned_row[12])
                                elif current_section == 'KUMPULAN':
                                    # Convert Tarikh Disenaraikan
                                    cleaned_row[6] = convert_date_format(cleaned_row[6])
                                
                                sections[current_section]['data'].append(cleaned_row)
            
            # Convert to DataFrames
            for section, section_data in sections.items():
                if section_data['data']:
                    df = pd.DataFrame(section_data['data'], columns=section_data['columns'])
                    extracted_tables[section] = df
    
    except Exception as e:
        logger.error(f"Error processing PDF: {e}")
        logger.error(traceback.format_exc())
        return {}
    
    return extracted_tables

def save_tables_to_csv(tables, output_dir='data/datasets/my_moha_sanctions'):
    """
    Save extracted tables to CSV files.
    
    Args:
        tables (dict): Dictionary of DataFrames
        output_dir (str): Directory to save CSV files
    """
    os.makedirs(output_dir, exist_ok=True)
    
    for section, df in tables.items():
        # Create safe filename
        safe_section = re.sub(r'[^\w\-_\. ]', '_', section)
        csv_filename = os.path.join(output_dir, f'{safe_section}.csv')
        
        # Explicitly add header row
        df.to_csv(csv_filename, index=False, encoding='utf-8')
        print(f"Saved {csv_filename}: {len(df)} rows (excluding header)")

def main(pdf_path):
    """
    Main function to process PDF and save tables to CSV.
    
    Args:
        pdf_path (str): Path to the PDF file
    """
    tables = extract_pdf_tables(pdf_path)
    save_tables_to_csv(tables)

if __name__ == '__main__':
    import sys
    import traceback
    
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Please provide a PDF file path.")
