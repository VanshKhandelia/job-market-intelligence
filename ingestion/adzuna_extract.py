import requests
import os
import json
import pandas as pd
from dotenv import load_dotenv
import time
from datetime import datetime

load_dotenv()

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")

# Our 50 target companies
COMPANIES = [
    "Fortinet", "CGI", "OpenText", "Autodesk", "Telus",
    "Global Relay", "Rogers", "Celestica", "Kinaxis", "Salesforce",
    "Softchoice", "Staples", "Huawei Technologies Canada Co", "Hootsuite",
    "Mitel","Google", "Amazon", "IBM",
    "SAP", "Deloitte", "Accenture"
]

BASE_URL = "https://api.adzuna.com/v1/api/jobs/ca/search"

def fetch_jobs_for_company(company: str, pages: int = 10) -> list:
    """Fetch job postings for a single company — pulls up to 10 pages"""
    all_jobs = []

    for page in range(1, pages + 1):
        params = {
            "app_id": APP_ID,
            "app_key": APP_KEY,
            "company": company,        # search in job title/description
            "results_per_page": 50,
            "content-type": "application/json"
        }

        try:
            response = requests.get(f"{BASE_URL}/{page}", params=params)
            response.raise_for_status()
            data = response.json()
            jobs = data.get("results", [])

            if not jobs:
                break  # no more results for this company

            for job in jobs:
                all_jobs.append({
                    "company_name_searched": company,
                    "job_id": job.get("id"),
                    "job_title": job.get("title"),
                    "company": job.get("company", {}).get("display_name"),
                    "location": job.get("location", {}).get("display_name"),
                    "description": job.get("description"),
                    "salary_min": job.get("salary_min"),
                    "salary_max": job.get("salary_max"),
                    "contract_type": job.get("contract_type"),
                    "contract_time": job.get("contract_time"),
                    "category": job.get("category", {}).get("label"),
                    "created": job.get("created"),
                    "redirect_url": job.get("redirect_url"),
                    "extracted_at": datetime.utcnow().isoformat()
                })

            print(f"  ✓ {company} — page {page}: {len(jobs)} jobs")
            time.sleep(0.5)  # be polite to the API

        except Exception as e:
            print(f"  ✗ Error fetching {company} page {page}: {e}")
            break

    return all_jobs


def run_extraction():
    """Run extraction for all companies and save to CSV"""
    all_jobs = []

    print(f"\n🚀 Starting extraction for {len(COMPANIES)} companies...\n")

    for company in COMPANIES:
        print(f"Fetching: {company}")
        jobs = fetch_jobs_for_company(company)
        all_jobs.extend(jobs)
        print(f"  → Total so far: {len(all_jobs)} jobs\n")
        time.sleep(1)  # pause between companies

    # Save raw to CSV as backup
    df = pd.DataFrame(all_jobs)
    df.drop_duplicates(subset=["job_id"], inplace=True)  # remove dupes
    
    output_path = "data/raw_jobs.csv"
    os.makedirs("data", exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"\n✅ Done! {len(df)} unique jobs saved to {output_path}")
    print(f"Companies covered: {df['company_name_searched'].nunique()}")
    print(f"Job categories found: {df['category'].value_counts().head(10)}")

    return df


if __name__ == "__main__":
    df = run_extraction()