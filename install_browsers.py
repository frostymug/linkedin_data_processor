import asyncio
from playwright.async_api import async_playwright
import subprocess
import sys

def install_browsers():
    """Install Playwright browsers using Python's module system"""
    print("Installing Playwright browsers...")
    try:
        # Run the installation command using Python's module system
        subprocess.run([sys.executable, '-m', 'playwright', 'install'], check=True)
        print("Playwright browsers installed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Error installing Playwright browsers: {e}")
        sys.exit(1)

if __name__ == '__main__':
    install_browsers()
