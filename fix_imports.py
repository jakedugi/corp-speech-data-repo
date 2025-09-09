#!/usr/bin/env python3
"""
Bulk fix import paths across the codebase.

Replaces:
- corpus_api.client.base_api_client → corpus_api.client.base_api_client
- corpus_types.schemas.models → corpus_types.schemas.models
- corpus_cleaner.cleaner → corpus_cleaner.cleaner
And other similar patterns.
"""

import pathlib
import re

def fix_imports_in_file(file_path):
    """Fix import paths in a single file."""
    content = file_path.read_text()

    # Define replacement patterns
    replacements = [
        # Generic pattern for any corp_speech_risk_dataset import
        (r'corp_speech_risk_dataset\.api\.client\.base_api_client',
         'corpus_api.client.base_api_client'),
        (r'corp_speech_risk_dataset\.api\.client\.ftc_client',
         'corpus_api.client.ftc_client'),
        (r'corp_speech_risk_dataset\.api\.client\.sec_client',
         'corpus_api.client.sec_client'),
        (r'corp_speech_risk_dataset\.api\.config\.courtlistener_config',
         'corpus_api.config.courtlistener_config'),
        (r'corp_speech_risk_dataset\.api\.config\.rss_config',
         'corpus_api.config.rss_config'),
        (r'corp_speech_risk_dataset\.api\.orchestrators\.courtlistener_orchestrator',
         'corpus_api.orchestrators.courtlistener_orchestrator'),
        (r'corp_speech_risk_dataset\.api\.orchestrators\.rss_orchestrator',
         'corpus_api.orchestrators.rss_orchestrator'),

        # corpus_types imports
        (r'corp_speech_risk_dataset\.types\.schemas\.models',
         'corpus_types.schemas.models'),
        (r'corp_speech_risk_dataset\.types\.schemas\.base_types',
         'corpus_types.schemas.base_types'),
        (r'corp_speech_risk_dataset\.types\.schemas\.quote_candidate',
         'corpus_types.schemas.quote_candidate'),
        (r'corp_speech_risk_dataset\.types\.misc\.base_types',
         'corpus_types.misc.base_types'),
        (r'corp_speech_risk_dataset\.types\.misc\.ports',
         'corpus_types.misc.ports'),
        (r'corp_speech_risk_dataset\.types\.misc\.quote_candidate',
         'corpus_types.misc.quote_candidate'),

        # corpus_cleaner imports
        (r'corp_speech_risk_dataset\.cleaner',
         'corpus_cleaner.cleaner'),
        (r'corp_speech_risk_dataset\.cleaner\.cleaner',
         'corpus_cleaner.cleaner'),

        # corpus_extractors imports
        (r'corp_speech_risk_dataset\.extractors\.quote_extractor',
         'corpus_extractors.quote_extractor'),
        (r'corp_speech_risk_dataset\.extractors\.first_pass',
         'corpus_extractors.first_pass'),
        (r'corp_speech_risk_dataset\.extractors\.attribution',
         'corpus_extractors.attribution'),
        (r'corp_speech_risk_dataset\.extractors\.rerank',
         'corpus_extractors.rerank'),
        (r'corp_speech_risk_dataset\.extractors\.models',
         'corpus_extractors.models'),

        # CLI imports
        (r'corp_speech_risk_dataset\.cli\.fetch',
         'corpus_api.cli.fetch'),
        (r'corp_speech_risk_dataset\.cli\.normalize',
         'corpus_cleaner.cli.normalize'),
        (r'corp_speech_risk_dataset\.cli\.extract',
         'corpus_extractors.cli.extract'),

        # Test imports
        (r'import corp_speech_risk_dataset\.cleaner',
         'import corpus_cleaner.cleaner'),
        (r'from corp_speech_risk_dataset\.cleaner import',
         'from corpus_cleaner.cleaner import'),
    ]

    # Apply replacements
    modified = False
    for pattern, replacement in replacements:
        new_content = re.sub(pattern, replacement, content)
        if new_content != content:
            content = new_content
            modified = True

    if modified:
        file_path.write_text(content)
        return True
    return False

def main():
    """Fix imports across the entire codebase."""
    root_dir = pathlib.Path('.')

    # Find all Python files (excluding some directories)
    exclude_patterns = [
        '__pycache__',
        '.git',
        'node_modules',
        '.pytest_cache',
        'htmlcov',
        'dist',
        'build',
        '*.egg-info'
    ]

    fixed_files = []

    for py_file in root_dir.rglob('*.py'):
        # Skip excluded patterns
        skip = False
        for pattern in exclude_patterns:
            if pattern in str(py_file):
                skip = True
                break
        if skip:
            continue

        if fix_imports_in_file(py_file):
            fixed_files.append(py_file)
            print(f"✅ Fixed {py_file}")

    print(f"\n📊 Fixed {len(fixed_files)} files with import issues")

if __name__ == "__main__":
    main()
