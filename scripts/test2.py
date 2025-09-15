#!/usr/bin/env python3
"""
Wonderwall Preview Downloader
Downloads the 30-second preview of Oasis - Wonderwall using SpotifyScraper
"""

import os
import sys
from datetime import datetime

try:
    from spotify_scraper import SpotifyClient
except ImportError:
    print("❌ SpotifyScraper not installed!")
    print("Install it with: pip install spotifyscraper")
    sys.exit(1)

def download_wonderwall():
    """Download Wonderwall preview and get track info"""
    
    # Wonderwall track URL (the example track ID you used earlier)
    wonderwall_url = "https://open.spotify.com/track/1qPbGZqppFwLwcBC1JQ6Vr"
    
    # Create download directory
    download_dir = "downloads"
    os.makedirs(download_dir, exist_ok=True)
    
    print("🎵 Wonderwall Preview Downloader")
    print("=" * 40)
    
    # Initialize client
    client = SpotifyClient()
    
    try:
        print("📡 Fetching track information...")
        
        # Get track metadata
        track_info = client.get_track_info(wonderwall_url)
        
        if track_info:
            print(f"🎼 Track: {track_info.get('name', 'Unknown')}")
            print(f"🎤 Artist: {track_info.get('artists', [{}])[0].get('name', 'Unknown') if track_info.get('artists') else 'Unknown'}")
            print(f"💿 Album: {track_info.get('album', {}).get('name', 'Unknown')}")
            print(f"⏱️  Duration: {track_info.get('duration_ms', 0) / 1000:.0f} seconds")
            print(f"📅 Release Date: {track_info.get('album', {}).get('release_date', 'Unknown')}")
        else:
            print("⚠️  Could not fetch track information")
        
        print("\n🎧 Downloading 30-second preview...")
        
        # Download the preview
        preview_path = client.download_preview_mp3(
            wonderwall_url, 
            path=download_dir,
            filename="wonderwall_preview.mp3"
        )
        
        if preview_path:
            # Get file size
            file_size = os.path.getsize(preview_path)
            
            print(f"✅ Successfully downloaded!")
            print(f"📁 File: {preview_path}")
            print(f"📏 Size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
            print(f"🕒 Downloaded at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Verify it's actually an audio file
            if preview_path.endswith('.mp3') and file_size > 1000:  # At least 1KB
                print("🔍 File appears to be valid MP3")
            else:
                print("⚠️  Warning: File might not be a valid audio file")
                
        else:
            print("❌ Failed to download preview")
            print("💡 This could mean:")
            print("   • No preview available for this track")
            print("   • Rate limiting by Spotify")
            print("   • Network connectivity issues")
    
    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        print("💡 Try running the script again in a few minutes")
    
    finally:
        # Always close the client
        client.close()
        print("\n🔌 SpotifyClient closed")

def test_installation():
    """Test if SpotifyScraper is working properly"""
    print("🧪 Testing SpotifyScraper installation...")
    
    try:
        client = SpotifyClient()
        print("✅ SpotifyClient initialized successfully")
        client.close()
        return True
    except Exception as e:
        print(f"❌ SpotifyClient test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting Wonderwall download script...")
    print(f"📍 Working directory: {os.getcwd()}")
    
    # Test installation first
    if test_installation():
        print()
        download_wonderwall()
    else:
        print("\n❌ Cannot proceed - SpotifyScraper not working properly")
        print("💡 Try reinstalling: pip install --upgrade spotifyscraper")
    
    print("\n✨ Script completed!")