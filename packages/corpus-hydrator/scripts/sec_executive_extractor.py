#!/usr/bin/env python3
"""
Extract current officer names, titles, and board member information from SEC DEF 14A filings
"""

import requests
import csv
import json
import time
import re
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SECExecutiveExtractor:
    """Extract executive and board member information from SEC filings"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "CorpSpeechDataRepo/1.0 (jake@jakedugan.com)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })

        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.2  # 5 requests per second max

    def _rate_limit(self):
        """Ensure we don't exceed SEC rate limits"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    def get_company_submissions(self, cik):
        """Get company submissions data from SEC"""
        self._rate_limit()
        url = f"https://data.sec.gov/submissions/CIK{cik:0>10}.json"

        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Failed to get submissions for CIK {cik}: {e}")
            return None

    def find_latest_def14a(self, submissions_data):
        """Find the most recent DEF 14A filing"""
        if not submissions_data:
            return None

        filings = submissions_data.get('filings', {}).get('recent', {})
        forms = filings.get('form', [])
        accessions = filings.get('accessionNumber', [])
        filing_dates = filings.get('filingDate', [])
        primary_docs = filings.get('primaryDocument', [])

        # Find DEF 14A filings
        def14a_indices = [i for i, form in enumerate(forms) if form == 'DEF 14A']

        if def14a_indices:
            # Get the most recent one
            latest_idx = def14a_indices[0]
            return {
                'accession': accessions[latest_idx],
                'date': filing_dates[latest_idx],
                'primary_doc': primary_docs[latest_idx]
            }

        return None

    def extract_executives_from_def14a(self, cik, def14a_info):
        """Extract executive information from DEF 14A filing"""
        if not def14a_info:
            return []

        accession = def14a_info['accession']
        primary_doc = def14a_info['primary_doc']

        # Construct document URL
        doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}/{primary_doc}"

        try:
            self._rate_limit()
            response = self.session.get(doc_url, timeout=30)
            response.raise_for_status()

            logger.info(f"Successfully accessed DEF 14A: {accession} ({len(response.content)} bytes)")

            # Parse HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract executive information
            executives = []

            # Method 1: Look for compensation tables (most reliable)
            executives.extend(self._extract_from_compensation_table(soup, cik))

            # Method 2: Look for executive officer sections
            if not executives:
                executives.extend(self._extract_from_executive_sections(soup, cik))

            # Method 3: Look for named executive officer disclosures
            if not executives:
                executives.extend(self._extract_named_executives(soup, cik))

            # Remove duplicates based on name
            seen_names = set()
            unique_executives = []
            for exec_info in executives:
                name_key = exec_info['name'].lower().strip()
                if name_key not in seen_names:
                    seen_names.add(name_key)
                    unique_executives.append(exec_info)

            logger.info(f"Extracted {len(unique_executives)} executives from DEF 14A")
            return unique_executives

        except Exception as e:
            logger.error(f"Failed to extract from DEF 14A {accession}: {e}")
            return []

    def _extract_from_compensation_table(self, soup, cik):
        """Extract executives from compensation tables (most accurate method)"""
        executives = []

        # Look for tables with compensation data
        tables = soup.find_all('table')

        for table in tables:
            # Check if this looks like a compensation table
            headers = table.find_all(['th', 'td'])
            header_text = ' '.join([h.get_text().strip().lower() for h in headers])

            # Look for compensation-related keywords
            comp_keywords = ['salary', 'bonus', 'stock', 'option', 'compensation', 'pay', 'executive']
            if any(keyword in header_text for keyword in comp_keywords):

                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        name_cell = cells[0].get_text().strip()

                        # Skip if this looks like a section header or non-name
                        if self._is_section_header(name_cell):
                            continue

                        # Clean up the name
                        name = self._clean_executive_name(name_cell)

                        if name and self._is_likely_person_name(name):
                            # Try to determine title from context or table structure
                            title = self._extract_title_from_context(row, table)

                            executives.append({
                                'cik': cik,
                                'name': name,
                                'title': title or 'Executive',
                                'source': 'DEF 14A Compensation Table',
                                'confidence': 'high'
                            })

        return executives

    def _is_section_header(self, text):
        """Check if text looks like a section header rather than a person name"""
        text_lower = text.lower().strip()

        # Common section headers to skip
        skip_patterns = [
            'letter to', 'notice of', 'proxy highlights', 'corporate governance',
            'elect the', 'nominees for', 'board membership', 'director nominee',
            'diversity of', 'board self', 'director nomination', 'identification',
            'shareholder nominations', 'director orientation', 'director independence',
            'corporate governance', 'board committees', 'executive committee',
            'compensation committee', 'audit committee', 'nominating committee',
            'executive officers', 'principal executive', 'chief executive',
            'chief financial', 'principal financial', 'executive compensation',
            'summary compensation', 'compensation discussion', 'pay ratio',
            'stock awards', 'option awards', 'equity compensation',
            'pension benefits', 'nonqualified', 'retirement plan',
            'potential payments', 'termination', 'change in control',
            'director compensation', 'fees earned', 'stock ownership',
            'beneficial ownership', 'security ownership', 'equity plan',
            'proposal', 'proposals', 'voting items', 'advisory vote',
            'say on pay', 'frequency vote', 'ratification', 'appointment',
            'audit firm', 'auditor', 'ratify', 'advisory', 'frequency',
            'stockholder', 'shareholder', 'annual meeting', 'meeting of',
            'information about', 'additional information', 'householding',
            'solicitation', 'voting', 'quorum', 'tabulation', 'inspector',
            'exhibit', 'annex', 'appendix', 'schedule', 'table of contents',
            'index', 'glossary', 'definitions', 'footnotes', 'notes',
            'summary', 'overview', 'introduction', 'background', 'purpose',
            'scope', 'methodology', 'assumptions', 'limitations', 'conclusions',
            'name of', 'non-employee director', 'employee director', 'director name',
            'principal executive officer', 'principal financial officer',
            'average total', 'median total', 'peer group', 'industry group',
            'compensation tables', 'elements of', 'compensation program',
            'total compensation', 'compensation mix', 'pay-for-performance'
        ]

        return any(pattern in text_lower for pattern in skip_patterns)

    def _is_likely_person_name(self, name):
        """Check if text looks like a person name"""
        if not name or len(name) < 3:
            return False

        # Must have at least first and last name
        parts = name.split()
        if len(parts) < 2:
            return False

        # First name should start with capital letter
        if not parts[0][0].isupper():
            return False

        # Last name should start with capital letter
        if not parts[-1][0].isupper():
            return False

        # Should not contain numbers
        if any(char.isdigit() for char in name):
            return False

        # Should not be too long (likely not a person name)
        if len(name) > 50:
            return False

        # Should not contain too many words (likely not a person name)
        if len(parts) > 4:
            return False

        return True

    def _extract_from_executive_sections(self, soup, cik):
        """Extract executives from executive officer sections"""
        executives = []

        # Find sections about executive officers
        text_content = soup.get_text().lower()

        # Look for executive officer mentions
        exec_patterns = [
            r'executive officer[s]?[:\s]*([^.]{10,100}?)(?:\n|\.|and|or|$)',
            r'principal executive officer[:\s]*([^.]{10,100}?)(?:\n|\.|and|or|$)',
            r'chief executive officer[:\s]*([^.]{10,100}?)(?:\n|\.|and|or|$)',
            r'chief financial officer[:\s]*([^.]{10,100}?)(?:\n|\.|and|or|$)',
        ]

        for pattern in exec_patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            for match in matches:
                name = self._clean_executive_name(match.strip())
                if name and self._is_likely_person_name(name):
                    # Determine title from pattern
                    title = 'Executive Officer'
                    if 'chief executive' in pattern:
                        title = 'Chief Executive Officer'
                    elif 'chief financial' in pattern:
                        title = 'Chief Financial Officer'
                    elif 'principal executive' in pattern:
                        title = 'Principal Executive Officer'

                    executives.append({
                        'cik': cik,
                        'name': name,
                        'title': title,
                        'source': 'DEF 14A Executive Section',
                        'confidence': 'medium'
                    })

        return executives

    def _extract_named_executives(self, soup, cik):
        """Extract named executive officers from general content"""
        executives = []

        # Look for common executive titles followed by names
        title_patterns = [
            (r'Chief Executive Officer[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)', 'Chief Executive Officer'),
            (r'Chief Financial Officer[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)', 'Chief Financial Officer'),
            (r'President[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)', 'President'),
            (r'Chief Operating Officer[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)', 'Chief Operating Officer'),
        ]

        text_content = soup.get_text()

        for pattern, title in title_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                name = match.strip()
                if name and self._is_likely_person_name(name):
                    executives.append({
                        'cik': cik,
                        'name': name,
                        'title': title,
                        'source': 'DEF 14A Named Executive',
                        'confidence': 'medium'
                    })

        return executives

    def _clean_executive_name(self, name_text):
        """Clean and normalize executive name"""
        # Remove common prefixes/suffixes
        name = re.sub(r'^(Mr\.|Mrs\.|Ms\.|Dr\.)\s+', '', name_text, flags=re.IGNORECASE)
        name = re.sub(r'\s*\([^)]*\)\s*$', '', name)  # Remove parenthetical notes
        name = re.sub(r'\s*,\s*(?:Jr\.?|Sr\.?|III|II|IV)\s*$', '', name, flags=re.IGNORECASE)

        # Clean up extra whitespace
        name = ' '.join(name.split())

        # Basic validation - should have at least first and last name
        if len(name.split()) >= 2 and len(name) <= 100:
            return name

        return None

    def _extract_title_from_context(self, row, table):
        """Extract title from table context"""
        # Look for title information in nearby cells or table headers
        cells = row.find_all(['td', 'th'])

        for cell in cells:
            text = cell.get_text().strip().lower()
            if any(title in text for title in ['ceo', 'chief executive', 'president', 'cfo', 'chief financial']):
                if 'ceo' in text or 'chief executive' in text:
                    return 'Chief Executive Officer'
                elif 'cfo' in text or 'chief financial' in text:
                    return 'Chief Financial Officer'
                elif 'president' in text:
                    return 'President'

        return None

    def extract_board_members(self, soup, cik):
        """Extract board member information from DEF 14A"""
        board_members = []

        # Look for board member sections
        text_content = soup.get_text()

        # Common board member patterns
        board_patterns = [
            r'board of directors[:\s]*([^.]{20,500}?)(?:\n|\.|and|or|$)',
            r'directors?[:\s]*([^.]{20,500}?)(?:\n|\.|and|or|$)',
        ]

        for pattern in board_patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            for match in matches:
                # Split by common separators
                names = re.split(r'[,&]|\sand\s|\sor\s|;\s*', match)

        for name in names:
                name = self._clean_executive_name(name.strip())
                if name and self._is_likely_person_name(name):
                    board_members.append({
                        'cik': cik,
                        'name': name,
                        'title': 'Director',
                        'source': 'DEF 14A Board Section',
                        'confidence': 'medium'
                    })

        # Remove duplicates
        seen_names = set()
        unique_board = []
        for member in board_members:
            name_key = member['name'].lower().strip()
            if name_key not in seen_names:
                seen_names.add(name_key)
                unique_board.append(member)

        return unique_board

    def process_company(self, ticker, cik):
        """Process a single company to extract executives and board members"""
        logger.info(f"Processing {ticker} (CIK: {cik})")

        # Clean CIK
        cik_clean = cik.lstrip('0')
        if not cik_clean.isdigit():
            logger.error(f"Invalid CIK format: {cik}")
            return []

        # Get submissions data
        submissions = self.get_company_submissions(cik_clean)
        if not submissions:
            return []

        # Find latest DEF 14A
        def14a_info = self.find_latest_def14a(submissions)
        if not def14a_info:
            logger.warning(f"No DEF 14A found for {ticker}")
            return []

        logger.info(f"Found DEF 14A: {def14a_info['accession']} ({def14a_info['date']})")

        # Extract executives from DEF 14A
        executives = self.extract_executives_from_def14a(cik_clean, def14a_info)

        # Try to extract board members too
        try:
            # We need to re-fetch the document for board extraction
            accession = def14a_info['accession']
            primary_doc = def14a_info['primary_doc']
            doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{accession.replace('-', '')}/{primary_doc}"

            self._rate_limit()
            response = self.session.get(doc_url, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                board_members = self.extract_board_members(soup, cik_clean)
                executives.extend(board_members)
        except Exception as e:
            logger.warning(f"Failed to extract board members: {e}")

        # Add ticker to all records
        for exec_info in executives:
            exec_info['ticker'] = ticker

        return executives

    def process_multiple_companies(self, input_file, output_file, max_companies=None):
        """Process multiple companies from CSV file"""
        logger.info(f"Processing companies from {input_file}")

        executives_data = []

        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for i, row in enumerate(reader, 1):
                ticker = row.get('ticker', '').strip()
                cik = row.get('cik', '').strip()

                if not ticker or not cik:
                    continue

                if max_companies and i > max_companies:
                    break

                logger.info(f"[{i}] Processing {ticker}...")
                company_executives = self.process_company(ticker, cik)
                executives_data.extend(company_executives)

                # Small delay between companies
                time.sleep(0.5)

        # Save results
        if executives_data:
            # Get all field names
            fieldnames = set()
            for exec_info in executives_data:
                fieldnames.update(exec_info.keys())
            fieldnames = sorted(fieldnames)

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(executives_data)

            logger.info(f"Saved {len(executives_data)} executive records to {output_file}")

        return executives_data

def main():
    """Main extraction process"""
    print("🚀 SEC Executive & Board Member Information Extractor")
    print("=" * 65)

    extractor = SECExecutiveExtractor()

    # Process a sample of companies
    input_file = "data/sp500_aliases_sec_enhanced.csv"
    output_file = "data/sec_executives_board_members.csv"

    if Path(input_file).exists():
        # Process first 5 companies for testing
        executives_data = extractor.process_multiple_companies(input_file, output_file, max_companies=5)

        print("\n📊 Extraction Results:")
        print(f"   Total executives/board members found: {len(executives_data)}")

        if executives_data:
            # Show sample results
            print("\n📋 Sample Results:")
            sample = executives_data[:10]  # Show first 10
            for i, exec_info in enumerate(sample, 1):
                print(f"   {i}. {exec_info.get('ticker', 'N/A')} - {exec_info.get('name', 'N/A')} ({exec_info.get('title', 'N/A')})")

            # Statistics
            titles = {}
            sources = {}
            for exec_info in executives_data:
                title = exec_info.get('title', 'Unknown')
                source = exec_info.get('source', 'Unknown')
                titles[title] = titles.get(title, 0) + 1
                sources[source] = sources.get(source, 0) + 1

            print("\n📈 Title Distribution:")
            for title, count in sorted(titles.items(), key=lambda x: x[1], reverse=True):
                print(f"   {title}: {count}")

            print("\n🔍 Source Distribution:")
            for source, count in sorted(sources.items(), key=lambda x: x[1], reverse=True):
                print(f"   {source}: {count}")

        print(f"\n✅ Results saved to: {output_file}")

    else:
        print(f"❌ Input file not found: {input_file}")

    print("\n" + "=" * 65)
    print("🏁 SEC Executive Extraction Complete")

if __name__ == "__main__":
    main()
