#!/usr/bin/env python3
"""
Enhance company data with SEC information for better accuracy and completeness
"""

import csv
import json
import requests
import time
from urllib.parse import urlparse
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SECDataEnhancer:
    """Enhance company data using SEC APIs"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "CorpSpeechDataRepo/1.0 (jake@jakedugan.com)",
            "Accept": "application/json, text/html, */*",
            "Accept-Encoding": "gzip, deflate"
        })

        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 10 requests per second max

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
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Failed to get submissions for CIK {cik}: {e}")
            return None

    def get_company_facts(self, cik):
        """Get XBRL company facts from SEC"""
        self._rate_limit()
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:0>10}.json"

        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Failed to get facts for CIK {cik}: {e}")
            return None

    def extract_sec_metadata(self, submissions_data):
        """Extract useful metadata from SEC submissions"""
        if not submissions_data:
            return {}

        metadata = {}

        # Basic company info
        if 'name' in submissions_data:
            metadata['sec_entity_name'] = submissions_data['name']

        if 'sic' in submissions_data:
            metadata['sic_code'] = submissions_data['sic']

        if 'sicDescription' in submissions_data:
            metadata['sic_description'] = submissions_data['sicDescription']

        if 'category' in submissions_data:
            metadata['sec_category'] = submissions_data['category']

        if 'fiscalYearEnd' in submissions_data:
            metadata['fiscal_year_end'] = submissions_data['fiscalYearEnd']

        if 'stateOfIncorporation' in submissions_data:
            metadata['state_of_incorporation'] = submissions_data['stateOfIncorporation']

        # Business address
        addresses = submissions_data.get('addresses', {})
        if 'business' in addresses:
            business_addr = addresses['business']
            metadata['business_street1'] = business_addr.get('street1', '')
            metadata['business_street2'] = business_addr.get('street2', '')
            metadata['business_city'] = business_addr.get('city', '')
            metadata['business_state'] = business_addr.get('stateOrCountry', '')
            metadata['business_zip'] = business_addr.get('zipCode', '')
            metadata['business_phone'] = business_addr.get('phone', '')

        # Mailing address
        if 'mailing' in addresses:
            mailing_addr = addresses['mailing']
            metadata['mailing_street1'] = mailing_addr.get('street1', '')
            metadata['mailing_street2'] = mailing_addr.get('street2', '')
            metadata['mailing_city'] = mailing_addr.get('city', '')
            metadata['mailing_state'] = mailing_addr.get('stateOrCountry', '')
            metadata['mailing_zip'] = mailing_addr.get('zipCode', '')

        # Recent filings
        filings = submissions_data.get('filings', {}).get('recent', {})
        if filings.get('form'):
            forms = filings['form']
            accession_numbers = filings.get('accessionNumber', [])
            filing_dates = filings.get('filingDate', [])

            # Find recent DEF 14A and 10-K
            def14a_info = self._find_recent_filing(forms, accession_numbers, filing_dates, 'DEF 14A')
            if def14a_info:
                metadata['latest_def14a_accession'] = def14a_info['accession']
                metadata['latest_def14a_date'] = def14a_info['date']

            tenk_info = self._find_recent_filing(forms, accession_numbers, filing_dates, '10-K')
            if tenk_info:
                metadata['latest_10k_accession'] = tenk_info['accession']
                metadata['latest_10k_date'] = tenk_info['date']

        return metadata

    def _find_recent_filing(self, forms, accessions, dates, target_form):
        """Find the most recent filing of a specific type"""
        for i, form in enumerate(forms):
            if form == target_form and i < len(accessions) and i < len(dates):
                return {
                    'accession': accessions[i],
                    'date': dates[i]
                }
        return None

    def enhance_company_data(self, input_file, output_file):
        """Enhance company data with SEC information"""
        logger.info(f"Enhancing company data from {input_file}")

        enhanced_data = []

        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            total_companies = sum(1 for _ in reader)
            f.seek(0)
            reader = csv.DictReader(f)

            for i, row in enumerate(reader, 1):
                cik = row.get('cik', '').strip()
                if not cik:
                    logger.warning(f"No CIK for {row.get('ticker', 'Unknown')}, skipping")
                    enhanced_data.append(row)
                    continue

                logger.info(f"[{i}/{total_companies}] Processing {row.get('ticker', 'Unknown')} (CIK: {cik})")

                # Clean CIK (remove leading zeros for API calls, but keep original format)
                cik_clean = cik.lstrip('0')
                if not cik_clean.isdigit():
                    logger.warning(f"Invalid CIK format: {cik}")
                    enhanced_data.append(row)
                    continue

                # Get SEC data
                submissions = self.get_company_submissions(cik_clean)
                sec_metadata = self.extract_sec_metadata(submissions)

                # Merge SEC data with existing data
                enhanced_row = dict(row)
                enhanced_row.update(sec_metadata)

                # Add processing timestamp
                enhanced_row['sec_data_updated'] = time.strftime('%Y-%m-%d %H:%M:%S')

                enhanced_data.append(enhanced_row)

        # Write enhanced data
        if enhanced_data:
            fieldnames = set()
            for row in enhanced_data:
                fieldnames.update(row.keys())
            fieldnames = sorted(fieldnames)

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(enhanced_data)

        logger.info(f"Enhanced data saved to {output_file}")
        return enhanced_data

def main():
    """Main enhancement process"""
    print("🚀 Enhancing Company Data with SEC Information")
    print("=" * 60)

    enhancer = SECDataEnhancer()

    # Enhance the basic company data
    input_file = "data/sp500_aliases.csv"
    output_file = "data/sp500_aliases_sec_enhanced.csv"

    if Path(input_file).exists():
        enhanced_data = enhancer.enhance_company_data(input_file, output_file)

        # Summary
        print("\n📊 Enhancement Summary:")
        print(f"   Total companies processed: {len(enhanced_data)}")

        # Count enhancements
        sec_names = sum(1 for row in enhanced_data if row.get('sec_entity_name'))
        sic_codes = sum(1 for row in enhanced_data if row.get('sic_code'))
        addresses = sum(1 for row in enhanced_data if row.get('business_city'))

        print(f"   Companies with SEC entity names: {sec_names}")
        print(f"   Companies with SIC codes: {sic_codes}")
        print(f"   Companies with business addresses: {addresses}")

        print(f"\n✅ Enhanced data saved to: {output_file}")

        # Show sample enhanced record
        sample = next((row for row in enhanced_data if row.get('sec_entity_name')), None)
        if sample:
            print("\n📋 Sample Enhanced Record:")
            print(f"   Ticker: {sample.get('ticker')}")
            print(f"   Original Name: {sample.get('official_name')}")
            print(f"   SEC Name: {sample.get('sec_entity_name', 'N/A')}")
            print(f"   SIC Code: {sample.get('sic_code', 'N/A')} - {sample.get('sic_description', '')}")
            print(f"   Business City: {sample.get('business_city', 'N/A')}")
    else:
        print(f"❌ Input file not found: {input_file}")

    print("\n" + "=" * 60)
    print("🏁 SEC Data Enhancement Complete")

if __name__ == "__main__":
    main()
