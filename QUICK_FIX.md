# Quick Fix Instructions

## Problem 1: GitHub detected secrets in files

**Solution:** The secrets are only in the README as instructions (in code blocks). GitHub is being overly cautious. You can safely ignore this warning OR remove those lines from README.md after you've copied them to GitHub Secrets.

## Problem 2: No Actions tab visible

**This happens when .github folder doesn't upload properly.**

### Fix Option A: Upload .github folder separately

1. Go to your repo: https://github.com/faupelfelix-cloud/leadintelligence
2. Click **"Add file"** → **"Upload files"**
3. From the extracted folder, drag ONLY the `.github` folder
4. Commit
5. Refresh page - Actions tab should appear

### Fix Option B: Create workflow manually

1. Go to: https://github.com/faupelfelix-cloud/leadintelligence
2. Click **"Actions"** tab (if you don't see it, go to Settings → Actions → General → Enable)
3. Click **"set up a workflow yourself"**
4. Delete the template, copy ALL content from `.github/workflows/enrich.yml` file
5. Name it: `enrich.yml`
6. Commit

### Fix Option C: Use Git command line

```bash
git clone https://github.com/faupelfelix-cloud/leadintelligence.git
cd leadintelligence

# Copy the .github folder from your extracted files
cp -r /path/to/extracted/.github .

git add .github
git commit -m "Add GitHub Actions workflow"
git push
```

---

## After Fix: How to Use

1. Go to: https://github.com/faupelfelix-cloud/leadintelligence
2. Click **"Actions"** tab (should now be visible!)
3. Click **"Enrich Leads"** workflow on the left
4. Click **"Run workflow"** button (right side)
5. Choose task from dropdown
6. Click green **"Run workflow"**
7. Watch it run!

---

## Your Secrets to Add

Go to: **Settings → Secrets and variables → Actions → New repository secret**

Add these 3 secrets:

**Secret 1:**
- Name: `AIRTABLE_API_KEY`
- Value: `patFxyu1UEfGSEu3c.7c0fc2a5174c565b1f54d8d7552e5a811122ac11b214bfc780a3e099f31ed8ed`

**Secret 2:**
- Name: `AIRTABLE_BASE_ID`
- Value: `appGQ85IVay6Gb5uZ`

**Secret 3:**
- Name: `ANTHROPIC_API_KEY`
- Value: `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`
