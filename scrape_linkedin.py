from linkedin_scraper import LinkedInScraper
import argparse

def main():
    parser = argparse.ArgumentParser(description='LinkedIn Profile Scraper')
    
    # Credentials source
    parser.add_argument('--credentials-source', choices=['env', 'args', 'interactive'],
                        default='env',
                        help='Source for LinkedIn credentials (default: env)')
    
    # Credentials for args source
    parser.add_argument('--email', help='LinkedIn email (required with --credentials-source=args)')
    parser.add_argument('--password', help='LinkedIn password (required with --credentials-source=args)')
    
    # Database path
    parser.add_argument('--db-path', default='linkedin_data.db',
                        help='Path to SQLite database (default: linkedin_data.db)')
    
    # Scraping options
    parser.add_argument('--limit', type=int, help='Limit number of profiles to scrape')
    
    args = parser.parse_args()
    
    # Validate credentials source and arguments
    if args.credentials_source == 'args' and (not args.email or not args.password):
        parser.error("--email and --password are required with --credentials-source=args")
    
    try:
        # Create scraper instance
        scraper = LinkedInScraper(
            db_path=args.db_path,
            credentials_source=args.credentials_source,
            email=args.email if args.email else None,
            password=args.password if args.password else None
        )
        
        # Scrape data
        results = scraper.scrape_connections(limit=args.limit)
        print(f"Successfully scraped {len(results)} profiles")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        if hasattr(e, 'args') and len(e.args) > 0:
            print(f"Detailed error: {e.args[0]}")
        raise

if __name__ == '__main__':
    main()
