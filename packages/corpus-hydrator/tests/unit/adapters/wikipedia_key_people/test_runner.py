#!/usr/bin/env python3
"""
Test Runner for Wikipedia Key People Module

This script runs all unit tests for the wikipedia_key_people module
and provides a summary of test results.
"""

import sys
import subprocess
import os
from pathlib import Path

def run_tests():
    """Run all wikipedia_key_people tests."""
    print("🧪 Running Wikipedia Key People Unit Tests")
    print("=" * 50)

    # Get the test directory
    test_dir = Path(__file__).parent
    package_root = test_dir.parent.parent.parent

    # Change to package root for proper imports
    os.chdir(package_root)

    # Run pytest on the test directory
    cmd = [
        sys.executable, "-m", "pytest",
        str(test_dir),
        "-v",  # Verbose output
        "--tb=short",  # Shorter traceback
        "--color=yes",  # Colored output
        "--durations=10"  # Show slowest 10 tests
    ]

    print(f"Running command: {' '.join(cmd)}")
    print()

    try:
        result = subprocess.run(cmd, capture_output=False, text=True)

        print("\n" + "=" * 50)
        print("Test Results Summary")
        print("=" * 50)

        if result.returncode == 0:
            print("✅ All tests passed!")
        else:
            print("❌ Some tests failed!")
            print(f"Exit code: {result.returncode}")

        return result.returncode

    except KeyboardInterrupt:
        print("\n⚠️  Tests interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Error running tests: {e}")
        return 1

def run_specific_test(test_file):
    """Run a specific test file."""
    test_dir = Path(__file__).parent
    package_root = test_dir.parent.parent.parent

    os.chdir(package_root)

    test_path = test_dir / test_file
    if not test_path.exists():
        print(f"❌ Test file not found: {test_file}")
        return 1

    cmd = [
        sys.executable, "-m", "pytest",
        str(test_path),
        "-v",
        "--tb=short",
        "--color=yes"
    ]

    print(f"Running specific test: {test_file}")
    result = subprocess.run(cmd, capture_output=False, text=True)
    return result.returncode

def show_test_coverage():
    """Show test coverage information."""
    test_dir = Path(__file__).parent
    package_root = test_dir.parent.parent.parent

    os.chdir(package_root)

    # Try to run with coverage if available
    try:
        cmd = [
            sys.executable, "-m", "pytest",
            str(test_dir),
            "--cov=corpus_hydrator.adapters.wikipedia_key_people",
            "--cov-report=term-missing",
            "--cov-report=html:htmlcov",
            "-v"
        ]

        print("📊 Running tests with coverage...")
        result = subprocess.run(cmd, capture_output=False, text=True)
        return result.returncode

    except ImportError:
        print("⚠️  pytest-cov not available, running without coverage")
        return run_tests()

def main():
    """Main function."""
    if len(sys.argv) > 1:
        if sys.argv[1] == "coverage":
            return show_test_coverage()
        elif sys.argv[1] == "specific" and len(sys.argv) > 2:
            return run_specific_test(sys.argv[2])
        else:
            print("Usage:")
            print("  python test_runner.py              # Run all tests")
            print("  python test_runner.py coverage     # Run with coverage")
            print("  python test_runner.py specific <test_file>  # Run specific test")
            return 1

    return run_tests()

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
