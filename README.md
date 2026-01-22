# Lead Intelligence & Enrichment System

Automated lead and company intelligence gathering for biologics CDMO business development.

**✨ Run everything directly from GitHub - no local computer needed! ✨**

## Quick Start

### 1. Upload Files to GitHub

1. Go to https://github.com/new
2. Name: `lead-intelligence-system`, **Private** ✅
3. Click "Create repository"
4. Click **"uploading an existing file"**
5. **Download file above**, extract, drag **ALL files** (including .github folder)
6. Commit

### 2. Add Your API Keys to GitHub Secrets

Go to: **Your Repo → Settings → Secrets and variables → Actions**

Click **"New repository secret"** three times:

| Name | Value |
|------|-------|
| `AIRTABLE_API_KEY` | `patFxyu1UEfGSEu3c.7c0fc2a5174c565b1f54d8d7552e5a811122ac11b214bfc780a3e099f31ed8ed` |
| `AIRTABLE_BASE_ID` | `appGQ85IVay6Gb5uZ` |
| `ANTHROPIC_API_KEY` | `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` |

### 3. Add Leads to Airtable

- Open your Airtable base
- Paste leads into "Leads" table
- Minimum: CRM Lead ID, Lead Name, Company Name

### 4. Run Enrichment from GitHub

Go to: **Your Repo → Actions → "Enrich Leads" → Run workflow**

Choose what to run:
- **Test (5 companies)** - Test first! ✅
- **Enrich Companies** - Enrich all companies
- **Enrich Leads** - Enrich all leads
- **Enrich Both** - Companies + Leads
- **Validate Setup Only** - Check configuration

Click **"Run workflow"** and watch it run! 🚀

### 5. Check Results

- **In Airtable**: See enriched data appear in real-time
- **In GitHub**: Actions tab → Click the running workflow → See live logs
- **Download logs**: After completion, download enrichment.log from Artifacts

---

## How to Use

### Running from GitHub (Recommended - No Computer Needed!)

1. Go to your repo on GitHub
2. Click **"Actions"** tab
3. Click **"Enrich Leads"** workflow
4. Click **"Run workflow"** button
5. Select task from dropdown
6. (Optional) Enter limit number (e.g., "10" for testing)
7. Click green **"Run workflow"**
8. Watch progress in real-time!

**Example workflows:**
- **First time**: Run "Test (5 companies)" to verify setup
- **Daily**: Run "Enrich Companies" to process new leads
- **Weekly**: Run "Enrich Both" for full enrichment

### Running Locally (Optional)

If you prefer to run on your computer:

```bash
git clone https://github.com/YOUR_USERNAME/lead-intelligence-system.git
cd lead-intelligence-system
python setup.py
python enrich_companies.py --limit 5
```

---

## What It Does

### Company Enrichment
- Finds: website, LinkedIn, location, funding, pipeline
- Calculates: ICP Fit Score (0-100), Urgency Score (0-100)
- Cost: ~$0.05-0.10 per company

### Lead Enrichment  
- Finds: email addresses, job titles, LinkedIn profiles
- Validates: email format, URL validity
- Cost: ~$0.02-0.05 per lead

---

## Workflow Options Explained

| Option | What it does | When to use |
|--------|-------------|-------------|
| **Test (5 companies)** | Enriches 5 companies only | First time, testing changes |
| **Enrich Companies** | Enriches all companies with status "Not Enriched" | Daily/weekly for new companies |
| **Enrich Leads** | Enriches all leads with status "Not Enriched" | After company enrichment |
| **Enrich Both** | Runs both companies + leads | Full enrichment run |
| **Validate Setup Only** | Tests configuration without enriching | Troubleshooting |

**Limit field**: Enter a number to process only that many records (useful for testing)

---

## Cost Tracking

GitHub Actions shows you:
- ✅ How many records processed
- ✅ Success/failure counts
- ✅ Time taken
- ✅ Download logs for detailed analysis

Estimate costs:
- 100 companies × $0.08 = ~$8
- 100 leads × $0.03 = ~$3

Check actual usage: https://console.anthropic.com/

---

## Viewing Results

### In Airtable (Real-time)
1. Open your Airtable base
2. Go to "Companies" or "Leads" table
3. Filter by: Enrichment Status = "Enriched"
4. Sort by: ICP Fit Score (high to low)

### In GitHub (Logs)
1. Go to Actions → Click your workflow run
2. Click "enrich" job → See live output
3. After completion → Artifacts → Download "enrichment-log"

---

## Scheduling (Optional)

Want automatic enrichment? Add this to `.github/workflows/enrich.yml`:

```yaml
on:
  schedule:
    - cron: '0 9 * * 1'  # Every Monday at 9 AM
  workflow_dispatch:     # Keep manual trigger too
```

This will automatically enrich new leads every week!

---

## Troubleshooting

### Workflow fails with "Config file not found"
- Make sure you uploaded the `.github` folder
- Check that `config.example.yaml` exists in your repo

### "Authentication failed"
- Verify secrets are named exactly: `AIRTABLE_API_KEY`, `AIRTABLE_BASE_ID`, `ANTHROPIC_API_KEY`
- Check secrets have the correct values (no extra spaces)

### No records enriched
- Check Airtable: Are records set to "Not Enriched"?
- Run "Validate Setup Only" first to check configuration

### Want to see what happened
- Click on the workflow run in Actions tab
- Click "enrich" job to see detailed logs
- Download "enrichment-log" artifact for full details

---

## Files

- `.github/workflows/enrich.yml` - GitHub Actions workflow
- `setup.py` - Reads secrets and creates config
- `enrich_companies.py` - Company enrichment
- `enrich_leads.py` - Lead enrichment
- `validate_setup.py` - Configuration checker

---

## Documentation

- [Airtable Setup](docs/airtable_setup_guide.md) - Database configuration
- [Data Model](docs/data_model_diagram.md) - Table structure  
- [Technical Details](docs/BULLETPROOFING.md) - How it works

---

**🎉 No local setup needed - everything runs on GitHub! 🎉**
