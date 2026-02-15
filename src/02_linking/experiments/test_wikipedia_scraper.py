"""
Test Wikipedia Scraper

This script tests the Wikipedia scraper functionality with a small sample
of institutions to verify it works correctly.
"""

import sys
sys.path.insert(0, '/home/kurtluo/yannan/jmp/src/02_linking')

from scrape_wikipedia_companies import (
    WikipediaScraper,
    WikipediaInfoboxParser,
    WikidataClient,
    USER_AGENT
)

def test_infobox_parser():
    """Test the infobox parser with known examples."""
    print("=" * 80)
    print("Testing WikipediaInfoboxParser")
    print("=" * 80)

    # Test with a sample Wikipedia HTML (simplified test)
    test_html = """
    <html>
    <body>
    <table class="infobox">
        <tr>
            <th>Ticker symbol</th>
            <td>{{NASDAQ|GOOGL}}, {{NASDAQ|GOOG}}</td>
        </tr>
        <tr>
            <th>ISIN</th>
            <td>US02079K3059</td>
        </tr>
        <tr>
            <th>Type</th>
            <td>Public</td>
        </tr>
        <tr>
            <th>Industry</th>
            <td>Technology</td>
        </tr>
        <tr>
            <th>Parent company</th>
            <td>[[Alphabet Inc.]]</td>
        </tr>
    </table>
    </body>
    </html>
    """

    result = WikipediaInfoboxParser.parse_infobox(test_html)

    print("\nParsed infobox data:")
    print(f"  Ticker symbols: {result['ticker_symbols']}")
    print(f"  ISIN: {result['isin']}")
    print(f"  Company type: {result['company_type']}")
    print(f"  Industry: {result['industry']}")
    print(f"  Parent company: {result['parent_company']}")

    # Assertions
    assert 'GOOGL' in result['ticker_symbols'] or 'NASDAQ' in str(result['ticker_symbols']), \
        "Should extract ticker symbols"
    assert result['isin'] == 'US02079K3059', "Should extract ISIN"
    assert result['company_type'] == 'public', "Should identify as public company"

    print("\n✓ Infobox parser test passed!")


def test_wikidata_client():
    """Test the Wikidata client."""
    print("\n" + "=" * 80)
    print("Testing WikidataClient")
    print("=" * 80)

    client = WikidataClient(USER_AGENT)

    # Test with Google's Wikidata ID (Q95)
    # Google: Q95
    print("\nFetching Wikidata for Google (Q95)...")

    company_info = client.get_company_info('Q95')

    print("\nRetrieved company info:")
    print(f"  Ticker symbols: {company_info['ticker_symbols']}")
    print(f"  ISIN: {company_info['isin']}")
    print(f"  CIK: {company_info['cik']}")
    print(f"  Parent company Wikidata: {company_info['parent_company_wikidata']}")
    print(f"  Stock exchange: {company_info['stock_exchange']}")

    # Google should have tickers
    if company_info['ticker_symbols']:
        print(f"\n✓ Successfully retrieved ticker symbols: {company_info['ticker_symbols']}")
    else:
        print("\n⚠ No ticker symbols found (may need to check Wikidata)")

    # Check for CIK (Critical for SEC matching)
    if company_info['cik']:
        print(f"✓ Successfully retrieved CIK: {company_info['cik']}")
    else:
        print("⚠ No CIK found")


def test_wikipedia_scraper():
    """Test the full Wikipedia scraper."""
    print("\n" + "=" * 80)
    print("Testing WikipediaScraper")
    print("=" * 80)

    scraper = WikipediaScraper(
        user_agent=USER_AGENT,
        min_delay=1.0,
        max_delay=2.0
    )

    # Test with Google's Wikipedia page
    print("\nTesting with Google's Wikipedia page...")
    wikipedia_url = "https://en.wikipedia.org/wiki/Google"

    print(f"Fetching: {wikipedia_url}")
    company_info = scraper.scrape_company_page("test_institution_id", wikipedia_url)

    if company_info:
        print("\n✓ Successfully scraped Wikipedia page!")
        print("\nExtracted information:")
        print(f"  Institution ID: {company_info['institution_id']}")
        print(f"  Wikipedia URL: {company_info['wikipedia_url']}")
        print(f"  Wikidata ID: {company_info['wikidata_id']}")
        print(f"  Ticker symbols: {company_info['ticker_symbols']}")
        print(f"  ISIN: {company_info['isin']}")
        print(f"  CIK: {company_info['cik']}")
        print(f"  Parent company: {company_info['parent_company']}")
        print(f"  Company type: {company_info['company_type']}")
        print(f"  Industry: {company_info['industry']}")
        print(f"  Headquarters: {company_info['headquarters']}")
        print(f"  Stock exchange: {company_info['stock_exchange']}")
        print(f"  Scraped at: {company_info['scraped_at']}")

        # Check for critical fields
        if company_info['ticker_symbols']:
            print(f"\n✓ Found {len(company_info['ticker_symbols'])} ticker symbol(s)")
        if company_info['cik']:
            print(f"✓ Found CIK: {company_info['cik']} (Critical for SEC matching!)")
        if company_info['parent_company']:
            print(f"✓ Found parent company: {company_info['parent_company']}")
    else:
        print("\n✗ Failed to scrape Wikipedia page")

    print(f"\nScraper statistics:")
    print(f"  Total: {scraper.stats['total']}")
    print(f"  Success: {scraper.stats['success']}")
    print(f"  Failed: {scraper.stats['failed']}")
    print(f"  With tickers: {scraper.stats['with_tickers']}")
    print(f"  With CIK: {scraper.stats['with_cik']}")


def test_sample_institutions():
    """Test with a sample of actual institutions."""
    print("\n" + "=" * 80)
    print("Testing with Sample Institutions")
    print("=" * 80)

    import polars as pl

    # Load enriched institutions
    institutions_file = "/home/kurtluo/yannan/jmp/data/interim/publication_institutions_enriched.parquet"

    try:
        institutions_df = pl.read_parquet(institutions_file)

        # Filter for companies with Wikipedia URLs
        companies_with_wikipedia = institutions_df.filter(
            (pl.col('wikipedia_url').is_not_null()) &
            (pl.col('is_company') == 1)
        )

        print(f"\nFound {len(companies_with_wikipedia):,} companies with Wikipedia URLs")

        # Get top 5 by paper count
        top_companies = companies_with_wikipedia.sort('paper_count', descending=True).head(5)

        print("\nTop 5 companies by paper count:")
        for i, row in enumerate(top_companies.iter_rows(named=True), 1):
            print(f"\n{i}. {row['display_name']}")
            print(f"   Wikipedia: {row['wikipedia_url']}")
            print(f"   Papers: {row['paper_count']:,}")

        # Test scraping the top company
        if len(top_companies) > 0:
            print("\n" + "-" * 80)
            print("Testing scraper on top company...")
            print("-" * 80)

            top_company = top_companies.row(0, named=True)
            scraper = WikipediaScraper()

            company_info = scraper.scrape_company_page(
                top_company['institution_id'],
                top_company['wikipedia_url']
            )

            if company_info:
                print("\n✓ Successfully scraped top company!")
                print(f"\nExtracted from {top_company['display_name']}:")
                print(f"  Ticker symbols: {company_info['ticker_symbols']}")
                print(f"  CIK: {company_info['cik']}")
                print(f"  ISIN: {company_info['isin']}")
                print(f"  Parent company: {company_info['parent_company']}")
                print(f"  Company type: {company_info['company_type']}")
                print(f"  Industry: {company_info['industry']}")
            else:
                print("\n✗ Failed to scrape top company")

    except Exception as e:
        print(f"\nError testing with sample institutions: {e}")


def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("WIKIPEDIA SCRAPER TEST SUITE")
    print("=" * 80)

    try:
        # Test 1: Infobox parser
        test_infobox_parser()

        # Test 2: Wikidata client
        test_wikidata_client()

        # Test 3: Full scraper
        test_wikipedia_scraper()

        # Test 4: Sample institutions
        test_sample_institutions()

        print("\n" + "=" * 80)
        print("ALL TESTS COMPLETED")
        print("=" * 80)

    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
