import os
import time
import random
import json
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from typing import List, Dict, Optional, Tuple
import pandas as pd
from sqlalchemy import create_engine
import argparse
import getpass

# Credential sources
CREDENTIAL_SOURCES = {
    'env': 'Environment variables',
    'args': 'Command line arguments',
    'interactive': 'Interactive prompt'
}

class LinkedInScraper:
    def __init__(self, db_path: str = "linkedin_data.db", 
                 credentials_source: str = 'env',
                 email: Optional[str] = None,
                 password: Optional[str] = None):
        self.db_path = db_path
        self.engine = create_engine(f'sqlite:///{db_path}')
        self.base_url = 'https://www.linkedin.com/login'
        self.rate_limit_delay = 3  # seconds between requests
        self.max_retries = 3
        self.retry_delay = 5  # seconds between retries
        self.credentials_source = credentials_source
        
        # Initialize browser context
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        
        # Get credentials based on source
        self.email, self.password = self._get_credentials(email, password)

    def _get_credentials(self, email: Optional[str], password: Optional[str]) -> Tuple[str, str]:
        """Get credentials from various sources"""
        if self.credentials_source == 'args':
            if not email or not password:
                raise ValueError("Email and password must be provided with --email and --password when using args source")
            return email, password
        
        elif self.credentials_source == 'env':
            load_dotenv()
            email = os.getenv('LINKEDIN_EMAIL')
            password = os.getenv('LINKEDIN_PASSWORD')
            if not email or not password:
                raise ValueError("LinkedIn credentials not found in environment variables")
            return email, password
        
        elif self.credentials_source == 'interactive':
            # Get credentials from user input
            email = input("Enter your LinkedIn email: ")
            password = getpass.getpass("Enter your LinkedIn password: ")
            if not email or not password:
                raise ValueError("Email and password must be provided")
            return email, password
        
        else:
            raise ValueError(f"Unknown credentials source: {self.credentials_source}")

    def _initialize_browser(self):
        """Initialize Playwright browser with enhanced anti-detection settings"""
        print("Initializing browser...")
        try:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-notifications",
                    "--disable-extensions",
                    "--disable-gpu",
                    "--disable-software-rasterizer"
                ]
            )
            self.context = self.browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                java_script_enabled=True,
                ignore_https_errors=True,
                bypass_csp=True,
                permissions=['geolocation', 'notifications'],
                locale='en-US',
                timezone_id='America/Chicago'
            )
            self.page = self.context.new_page()
            
            # Add anti-detection measures
            self.page.evaluate("""
                delete navigator.__proto__.webdriver;
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                // Add more anti-detection measures
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
                
                Object.defineProperty(navigator, 'platform', {
                    get: () => 'Win32'
                });
                
                Object.defineProperty(navigator, 'plugins', {
                    get: () => []
                });
                
                // Add more advanced anti-detection
                Object.defineProperty(navigator, 'hardwareConcurrency', {
                    get: () => 4
                });
                
                Object.defineProperty(navigator, 'deviceMemory', {
                    get: () => 8
                });
                
                Object.defineProperty(navigator, 'userAgentData', {
                    get: () => null
                });
            """)
            
            # Add more anti-detection measures
            self.page.evaluate("""
                const originalQuerySelector = document.querySelector;
                document.querySelector = (selector) => {
                    const element = originalQuerySelector.call(document, selector);
                    if (element) {
                        const originalGetAttribute = element.getAttribute;
                        element.getAttribute = function(attr) {
                            if (attr === 'class' || attr === 'id') return null;
                            return originalGetAttribute.call(this, attr);
                        };
                    }
                    return element;
                };
            """)
            
            # Add random delay to mimic human behavior
            time.sleep(random.uniform(2, 3))
            
            print("Browser initialized successfully!")
            
        except Exception as e:
            print(f"Error initializing browser: {str(e)}")
            self._cleanup_browser()
            raise

    def _cleanup_browser(self):
        """Clean up browser resources"""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
        except Exception as e:
            print(f"Error during cleanup: {str(e)}")

    def _login(self) -> bool:
        """Login to LinkedIn using stored credentials"""
        try:
            print("=== Starting login process ===")
            print(f"Using credentials: {self.email[:5]}...{self.email[-5:]}")
            
            # Initialize browser if not already initialized
            if not self.page:
                print("Browser not initialized, initializing now...")
                self._initialize_browser()
            
            print("=== Navigating to LinkedIn login page ===")
            print(f"Base URL: {self.base_url}")
            self.page.goto(self.base_url)
            time.sleep(5)  # Increased wait time for login page
            
            # Wait for login form with retry mechanism
            print("=== Waiting for login form ===")
            max_retries = 3
            retry_delay = 3
            
            for attempt in range(max_retries):
                try:
                    # Try multiple selectors for login form
                    email_input = None
                    password_input = None
                    submit_button = None
                    
                    # Try different selectors
                    selectors = [
                        ('input[name="session_key"]', 'input[name="session_password"]', 'button[type="submit"]'),
                        ('input[name="username"]', 'input[name="password"]', 'button[type="submit"]'),
                        ('input[id="username"]', 'input[id="password"]', 'button[type="submit"]'),
                        ('input[placeholder="Email or Phone"]', 'input[placeholder="Password"]', 'button[type="submit"]'),
                        ('input[autocomplete="username"]', 'input[autocomplete="current-password"]', 'button[type="submit"]')
                    ]
                    
                    print(f"=== Attempt {attempt + 1}/{max_retries} ===")
                    for email_sel, pass_sel, submit_sel in selectors:
                        try:
                            print(f"Trying selectors: {email_sel}, {pass_sel}, {submit_sel}")
                            # Wait for elements to be visible
                            email_input = self.page.wait_for_selector(email_sel, timeout=10000, state='visible')
                            if email_input:
                                print("Found email input")
                            
                            password_input = self.page.wait_for_selector(pass_sel, timeout=10000, state='visible')
                            if password_input:
                                print("Found password input")
                            
                            submit_button = self.page.wait_for_selector(submit_sel, timeout=10000, state='visible')
                            if submit_button:
                                print("Found submit button")
                            
                            if email_input and password_input and submit_button:
                                print("Found all required elements!")
                                break
                        except Exception as e:
                            print(f"Error with selectors {email_sel}, {pass_sel}, {submit_sel}: {str(e)}")
                            continue
                    
                    if email_input and password_input and submit_button:
                        print("=== Found login form elements ===")
                        break
                        
                    print(f"Login form not found, retrying... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                    
                    # Refresh page if needed
                    if attempt > 0:
                        print("Refreshing page...")
                        self.page.reload()
                        time.sleep(4)
                except Exception as e:
                    print(f"Error finding login form: {str(e)}")
                    if attempt == max_retries - 1:
                        raise
            
            if not (email_input and password_input and submit_button):
                print("=== Failed to find login form elements ===")
                return False
            
            # Enter email
            print(f"=== Entering email: {self.email[:5]}...{self.email[-5:]} ===")
            email_input.fill(self.email)
            time.sleep(random.uniform(1.5, 2.5))
            
            # Enter password
            print("=== Entering password ===")
            password_input.fill(self.password)
            time.sleep(random.uniform(1.5, 2.5))
            
            # Click login button
            print("=== Clicking login button ===")
            submit_button.click()
            time.sleep(random.uniform(3, 4))
            
            # Check login status
            print("=== Checking login status ===")
            current_url = self.page.url
            print(f"Current URL: {current_url}")
            print(f"Page content: {self.page.content()[:500]}...")
            
            if current_url.startswith('https://www.linkedin.com/login'):
                print("=== Login failed - still on login page ===")
                return False
                
            # Check for 2FA or verification
            if current_url.startswith('https://www.linkedin.com/checkpoint'):
                print("=== 2FA or verification required ===")
                return False
                
            # Check if we're on a profile page or home page
            if (current_url.startswith('https://www.linkedin.com/in/') or 
                current_url.startswith('https://www.linkedin.com/feed/')):
                print("=== Login successful! ===")
                return True
                
            print("=== Login status unknown ===")
            print(f"Unexpected URL: {current_url}")
            return False
            
        except Exception as e:
            print(f"=== Login error: {str(e)} ===")
            return False
        finally:
            # Only cleanup if login failed
            if not self.page:
                self._cleanup_browser()

    def _login(self) -> bool:
        """Login to LinkedIn using stored credentials"""
        try:
            print("Attempting to login...")
            self.page.goto(self.base_url)
            time.sleep(2)
            
            # Wait for login form
            self.page.wait_for_selector('input[name="session_key"]', timeout=10000)
            
            # Enter email
            print(f"Entering email: {self.email[:5]}...{self.email[-5:]}")  # Mask email for security
            self.page.fill('input[name="session_key"]', self.email)
            time.sleep(random.uniform(1, 2))
            
            # Enter password
            print("Entering password...")
            self.page.fill('input[name="session_password"]', self.password)
            time.sleep(random.uniform(1, 2))
            
            # Click login button
            print("Clicking login button...")
            self.page.click('button[type="submit"]')
            time.sleep(random.uniform(2, 4))
            
            # Check for successful login
            print("Checking login status...")
            if self.page.url.startswith(self.base_url + '/login'):
                print("Login failed - still on login page")
                return False
                
            # Check for 2FA or verification
            if self.page.url.startswith(self.base_url + '/checkpoint'):
                print("2FA or verification required - please check your email")
                return False
                
            print("Login successful!")
            return True
            
        except Exception as e:
            print(f"Login error: {str(e)}")
            return False

    def _initialize_browser(self):
        """Initialize Playwright browser with anti-detection settings"""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox"
            ]
        )
        self.context = self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            java_script_enabled=True,
            ignore_https_errors=True
        )
        self.page = self.context.new_page()
        
        # Add anti-detection measures
        self.page.evaluate("""
            delete navigator.__proto__.webdriver;
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        # Add more anti-detection measures
        self.page.evaluate("""
            const originalQuerySelector = document.querySelector;
            document.querySelector = (selector) => {
                const element = originalQuerySelector.call(document, selector);
                if (element) {
                    const originalGetAttribute = element.getAttribute;
                    element.getAttribute = function(attr) {
                        if (attr === 'class') return null;
                        return originalGetAttribute.call(this, attr);
                    };
                }
                return element;
            };
        """)
        
        # Add random delay to mimic human behavior
        time.sleep(random.uniform(1, 2))

    def _login(self) -> bool:
        """Login to LinkedIn using stored credentials"""
        try:
            print("Starting login process...")
            
            # Initialize browser if not already initialized
            if not self.page:
                print("Browser not initialized, initializing now...")
                self._initialize_browser()
            
            print("Navigating to LinkedIn...")
            self.page.goto(self.base_url)
            time.sleep(2)
            
            # Wait for login form
            print("Waiting for login form...")
            self.page.wait_for_selector('input[name="session_key"]', timeout=10000)
            
            # Enter email
            print(f"Entering email: {self.email[:5]}...{self.email[-5:]}")
            self.page.fill('input[name="session_key"]', self.email)
            time.sleep(random.uniform(1, 2))
            
            # Enter password
            self.page.fill('input[name="session_password"]', self.password)
            time.sleep(random.uniform(1, 2))
            
            # Click login button
            self.page.click('button[type="submit"]')
            time.sleep(random.uniform(2, 4))
            
            # Check if login was successful
            if self.page.url.startswith(self.base_url + '/login'):
                return False
            return True
            
        except Exception as e:
            print(f"Login error: {str(e)}")
            return False

    def _scrape_profile(self, profile_url: str) -> Dict:
        """Scrape data from a LinkedIn profile"""
        try:
            print(f"Scraping profile: {profile_url}")
            # Navigate to profile
            self.page.goto(profile_url)
            print(f"After goto: Current URL: {self.page.url}")
            time.sleep(random.uniform(2, 4))
            
            # Scroll to load more content
            self.page.evaluate("""
                window.scrollTo(0, document.body.scrollHeight);
            """)
            print(f"After scroll: Page content length: {len(self.page.content())}")
            time.sleep(2)
            
            # Debug page state
            print(f"\n=== Page State Before Extraction ===")
            print(f"Current URL: {self.page.url}")
            print(f"Page title: {self.page.title()}")
            print(f"First 500 chars of content: {self.page.content()[:500]}...")
            print("\n=== Available Selectors ===")
            print(f"Found headline selector: {self.page.query_selector('h2.text-heading-xlarge') is not None}")
            print(f"Found summary selector: {self.page.query_selector('div.pv-about-section') is not None}")
            print("=== End Debug Info ===\n")
            
            # Extract experience and education
            experience = []
            education = []
            
            # Debug selectors
            print("\n=== Experience Section ===")
            # Try different selectors for experience
            exp_section = self.page.query_selector('section.pv-profile-section.pv-experience-section')
            print(f"Found experience section: {exp_section is not None}")
            
            if exp_section:
                exp_elements = exp_section.query_selector_all('li.pv-entity__position-group-pager')
                print(f"Found {len(exp_elements)} experience elements")
                
                for exp in exp_elements:
                    try:
                        print("\n=== Experience Item ===")
                        role = exp.query_selector('div.pv-entity__summary-info-v2 > h3')
                        print(f"Role selector: {role is not None}")
                        company = exp.query_selector('p.pv-entity__secondary-title')
                        print(f"Company selector: {company is not None}")
                        dates = exp.query_selector('div.pv-entity__date-range')
                        print(f"Dates selector: {dates is not None}")
                        location = exp.query_selector('div.pv-entity__location')
                        print(f"Location selector: {location is not None}")
                        
                        if role and company:
                            experience.append({
                                'role': role.text_content(),
                                'company': company.text_content(),
                                'dates': dates.text_content() if dates else None,
                                'location': location.text_content() if location else None
                            })
                    except Exception as e:
                        print(f"Error processing experience item: {str(e)}")
                        continue
            
            print("\n=== Education Section ===")
            # Try different selectors for education
            edu_section = self.page.query_selector('section.pv-profile-section.pv-education-section')
            print(f"Found education section: {edu_section is not None}")
            
            if edu_section:
                edu_elements = edu_section.query_selector_all('div.pv-entity__degree-info')
                print(f"Found {len(edu_elements)} education elements")
                
                for edu in edu_elements:
                    try:
                        print("\n=== Education Item ===")
                        school = edu.query_selector('h3.pv-entity__school-name')
                        print(f"School selector: {school is not None}")
                        degree = edu.query_selector('p.pv-entity__degree-name')
                        print(f"Degree selector: {degree is not None}")
                        field = edu.query_selector('p.pv-entity__fos')
                        print(f"Field selector: {field is not None}")
                        dates = edu.query_selector('p.pv-entity__dates')
                        print(f"Dates selector: {dates is not None}")
                        
                        if school:
                            education.append({
                                'school': school.text_content(),
                                'degree': degree.text_content() if degree else None,
                                'field': field.text_content() if field else None,
                                'dates': dates.text_content() if dates else None
                            })
                    except Exception as e:
                        print(f"Error processing education item: {str(e)}")
                        continue
            
            # Add debug information
            print(f"\n=== Scraped Data ===")
            print(f"Experience: {json.dumps(experience, indent=2)}")
            print(f"Education: {json.dumps(education, indent=2)}")
            print(f"=== End Scraped Data ===\n")
            
            return {
                'url': profile_url,
                'scraped_at': datetime.now().isoformat(),
                'experience': experience,
                'education': education
            }
            
            return data
            
        except Exception as e:
            print(f"Error scraping {profile_url}: {str(e)}")
            return {'url': profile_url, 'error': str(e)}

    def _scrape_recent_activity(self) -> List[Dict]:
        """Scrape recent activity from the profile"""
        try:
            activities = []
            activity_elements = self.page.query_selector_all('div.pv-recent-activity-item')
            
            for element in activity_elements[:5]:  # Limit to 5 recent activities
                activity = {
                    'type': element.query_selector_text('span.pv-recent-activity-item__type', strict=False),
                    'description': element.query_selector_text('span.pv-recent-activity-item__description', strict=False),
                    'timestamp': element.query_selector_text('time', strict=False)
                }
                activities.append(activity)
            
            return activities
            
        except:
            return []

    def _scrape_endorsements(self) -> List[Dict]:
        """Scrape endorsements from the profile"""
        try:
            endorsements = []
            endorsement_elements = self.page.query_selector_all('div.pv-skill-category-entity')
            
            for element in endorsement_elements:
                skill = {
                    'skill': element.query_selector_text('h3.pv-skill-category-entity__name', strict=False),
                    'endorsements_count': element.query_selector_text('span.pv-skill-category-entity__endorsement-count', strict=False)
                }
                endorsements.append(skill)
            
            return endorsements
            
        except:
            return []

    def _scrape_skills(self) -> List[str]:
        """Scrape skills from the profile"""
        try:
            skills = []
            skill_elements = self.page.query_selector_all('span.pv-skill-category-entity__name')
            
            for element in skill_elements:
                skill = element.text_content()
                skills.append(skill)
            
            return skills
            
        except Exception as e:
            print(f"Error in login: {str(e)}")
            return False

    def scrape_connections(self, limit: int = None) -> List[Dict]:
        """Scrape data for all connections in the database"""
        try:
            # Initialize browser and login
            self._initialize_browser()
            if not self._login():
                raise Exception("Failed to login to LinkedIn")
            
            # Get list of connections from database
            query = "SELECT * FROM connections LIMIT :limit" if limit is not None else "SELECT * FROM connections"
            params = {'limit': limit} if limit is not None else {}
            df = pd.read_sql(query, self.engine, params=params)
            
            # Get unique LinkedIn URLs
            urls = df['linkedin_url'].unique().tolist() if 'linkedin_url' in df.columns else []
            print(f"Found {len(urls)} LinkedIn URLs in database")
            
            # Scrape each profile
            results = []
            for url in urls:
                if not url:
                    continue
                
                # Add random delay between requests
                time.sleep(random.uniform(self.rate_limit_delay - 1, self.rate_limit_delay + 1))
                
                try:
                    profile_data = self._scrape_profile(url)
                    results.append(profile_data)
                    self._save_to_database(profile_data)
                except Exception as e:
                    print(f"Error scraping profile {url}: {str(e)}")
                    continue
            
            print(f"Successfully scraped {len(results)} profiles")
            return results
        except Exception as e:
            print(f"Error in scrape_connections: {str(e)}")
            raise
        finally:
            # Clean up browser resources
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()

# ... (rest of the code remains the same)
    def scrape_connections(self, limit: int = None) -> List[Dict]:
        """Scrape data for all connections in the database"""
        try:
            # Initialize browser and login
            self._initialize_browser()
            if not self._login():
                raise Exception("Failed to login to LinkedIn")
            
            # Get list of connections from database
            query = "SELECT * FROM connections LIMIT :limit" if limit is not None else "SELECT * FROM connections"
            params = {'limit': limit} if limit is not None else {}
            df = pd.read_sql(query, self.engine, params=params)
            
            # Get unique LinkedIn URLs
            urls = df['url'].unique().tolist() if 'url' in df.columns else []
            print(f"Found {len(urls)} LinkedIn URLs in database")
            
            # Scrape each profile
            results = []
            for url in urls:
                if not url:
                    continue
                
                # Add random delay between requests
                time.sleep(random.uniform(self.rate_limit_delay - 1, self.rate_limit_delay + 1))
                
                try:
                    profile_data = self._scrape_profile(url)
                    results.append(profile_data)
                    print(f"Profile data: {json.dumps(profile_data, indent=2)}")
                except Exception as e:
                    print(f"Error scraping profile {url}: {str(e)}")
                    continue
            
            print(f"Successfully scraped {len(results)} profiles")
            return results
        except Exception as e:
            print(f"Error in scrape_connections: {str(e)}")
            raise
        finally:
            # Clean up browser resources
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()

    def __del__(self):
        """Clean up browser resources"""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
        except:
            pass

if __name__ == '__main__':
    # Example usage
    scraper = LinkedInScraper()
    try:
        # Scrape data for first 10 connections
        results = scraper.scrape_connections(limit=10)
        print(f"Scraped {len(results)} profiles")
    except Exception as e:
        print(f"Error running scraper: {str(e)}")
