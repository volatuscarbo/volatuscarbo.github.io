# .github/scripts/sync_to_supabase.py

import os
import json
import requests
from datetime import datetime
from pathlib import Path

# Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY')
GITHUB_REPO = os.getenv('GITHUB_REPO')
GITHUB_BRANCH = os.getenv('GITHUB_BRANCH', 'main')

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("❌ Error: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    exit(1)

# Supabase API headers
headers = {
    'apikey': SUPABASE_SERVICE_KEY,
    'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
    'Content-Type': 'application/json'
}

# Paths in your repo
BASELINES_DIR = 'ETS_LEGAL/baselines'
DIFFS_FILE = 'ETS_LEGAL/data/diffs.json'

def get_file_hash(filepath):
    """Generate a simple hash of file contents"""
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        return hash(content) % (10 ** 8)
    except:
        return None

def get_raw_github_url(filepath):
    """Generate GitHub raw content URL"""
    return f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{filepath}"

def load_diffs_metadata():
    """Load metadata from diffs.json"""
    diffs_data = {}
    if os.path.exists(DIFFS_FILE):
        try:
            with open(DIFFS_FILE, 'r', encoding='utf-8') as f:
                full_data = json.load(f)
            print(f"📊 Loaded {DIFFS_FILE}")
            
            # Parse diffs data - adjust based on your actual structure
            # This assumes structure like: {"documents": {...}} or similar
            if isinstance(full_data, dict):
                diffs_data = full_data
            
            return diffs_data
        except Exception as e:
            print(f"⚠️  Could not load diffs.json: {e}")
    
    return diffs_data

def sync_documents():
    """Main sync function"""
    print(f"🔄 Starting Supabase sync...")
    print(f"📍 Repo: {GITHUB_REPO}")
    print(f"🌿 Branch: {GITHUB_BRANCH}")
    print(f"📂 Baselines directory: {BASELINES_DIR}")
    print(f"📄 Diffs file: {DIFFS_FILE}")
    
    # Find all txt files in baselines directory
    if not os.path.exists(BASELINES_DIR):
        print(f"❌ Error: Baselines directory not found: {BASELINES_DIR}")
        return
    
    txt_files = list(Path(BASELINES_DIR).glob('*.txt'))
    
    if not txt_files:
        print(f"⚠️  No .txt files found in {BASELINES_DIR}")
        return
    
    print(f"📋 Found {len(txt_files)} documents in {BASELINES_DIR}")
    
    # Load diffs metadata
    diffs_data = load_diffs_metadata()
    
    # First, mark all existing documents as not latest
    print("🔍 Marking old versions...")
    try:
        response = requests.patch(
            f"{SUPABASE_URL}/rest/v1/documents?is_latest=eq.true",
            headers=headers,
            json={"is_latest": False}
        )
        if response.status_code == 204:
            print("✅ Marked old versions as not latest")
        else:
            print(f"⚠️  Warning marking old versions: {response.status_code}")
    except Exception as e:
        print(f"❌ Error marking old versions: {e}")
    
    # Now upsert current documents
    documents_synced = 0
    
    for txt_file in txt_files:
        filename = txt_file.name
        relative_path = f"{BASELINES_DIR}/{filename}"
        
        print(f"\n📄 Processing: {filename}")
        
        try:
            # Read file content
            with open(txt_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Generate file hash for versioning
            file_hash = str(get_file_hash(str(txt_file)))
            
            # Extract title and directive from filename or diffs metadata
            title = filename.replace('.txt', '').replace('_', ' ').title()
            directive = filename.replace('.txt', '').upper()
            
            # Try to get from diffs data if available
            if isinstance(diffs_data, dict):
                # Check various possible structures in diffs.json
                for key, value in diffs_data.items():
                    if isinstance(value, dict):
                        if value.get('name') == filename or value.get('file') == filename:
                            title = value.get('title', title)
                            directive = value.get('directive', directive)
                            print(f"   📌 Found metadata in diffs.json")
                            break
            
            # GitHub raw URL
            file_path = get_raw_github_url(relative_path)
            
            # GitHub web URL
            web_url = f"https://github.com/{GITHUB_REPO}/blob/{GITHUB_BRANCH}/{relative_path}"
            
            # Prepare document record
            document = {
                'title': title,
                'directive': directive,
                'version_date': datetime.utcnow().isoformat() + 'Z',
                'is_latest': True,
                'file_path': file_path,
                'web_url': web_url,
                'file_hash': file_hash,
                'version': file_hash
            }
            
            # Upsert to Supabase (insert or update if exists)
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/documents",
                headers=headers,
                json=document
            )
            
            if response.status_code in [200, 201]:
                print(f"✅ Synced: {title}")
                print(f"   📎 File: {file_path}")
                documents_synced += 1
            else:
                print(f"❌ Error syncing {filename}: {response.status_code}")
                print(f"   Response: {response.text}")
        
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")
    
    print(f"\n{'='*60}")
    print(f"✅ Sync Complete!")
    print(f"📊 Documents synced: {documents_synced}/{len(txt_files)}")
    print(f"🔗 Supabase URL: {SUPABASE_URL}")
    print(f"📂 Baselines synced from: {BASELINES_DIR}")
    print(f"{'='*60}")

if __name__ == '__main__':
    sync_documents()
