# Lead Intelligence System - Airtable Setup Guide

## Overview
This guide will help you set up the Airtable base for your Lead Intelligence & Enrichment System.

---

## Step 1: Create New Base

1. Go to airtable.com
2. Click "Create a base" → "Start from scratch"
3. Name it: **"Lead Intelligence System"**

---

## Step 2: Create Tables

You'll create 4 tables. Start by renaming the default table to "Companies"

### Table 1: COMPANIES

**Primary Field:** Company Name (Single line text)

**Fields to Add:**

| Field Name | Field Type | Options/Settings |
|------------|------------|------------------|
| Website | URL | - |
| LinkedIn Company Page | URL | - |
| Location/HQ | Single line text | - |
| Company Size | Single select | Options: 1-10, 11-50, 51-200, 201-500, 501-1000, 1000+ |
| Focus Area | Multiple select | Options: mAbs, Bispecifics, ADCs, Recombinant Proteins, Cell Therapy, Gene Therapy, Vaccines, Other |
| Technology Platform | Multiple select | Options: Mammalian CHO, Mammalian Non-CHO, Microbial, Cell-Free, Other |
| Funding Stage | Single select | Options: Seed, Series A, Series B, Series C, Series D+, Public, Acquired, Unknown |
| Total Funding | Currency | USD |
| Latest Funding Round | Single line text | (e.g., "Series B - $75M - Oct 2024") |
| Pipeline Stage | Multiple select | Options: Preclinical, Phase 1, Phase 2, Phase 3, Commercial, Unknown |
| Lead Programs | Long text | - |
| Therapeutic Areas | Multiple select | Options: Oncology, Autoimmune, Rare Disease, Infectious Disease, CNS, Metabolic, Other |
| Current CDMO Partnerships | Long text | - |
| Manufacturing Status | Single select | Options: No Public Partner, Has Partner, Building In-House, Unknown |
| Enrichment Status | Single select | Options: Not Enriched, In Progress, Enriched, Failed |
| ICP Fit Score | Number | Integer, 0-100 |
| Urgency Score | Number | Integer, 0-100 |
| Last Intelligence Check | Date | - |
| Intelligence Notes | Long text | - |
| Date Added | Created time | - |
| Last Modified | Last modified time | - |

**Create a View:**
- Name: "Need Enrichment"
- Filter: Enrichment Status = "Not Enriched"

---

### Table 2: LEADS

**Primary Field:** Lead Name (Single line text)

**Fields to Add:**

| Field Name | Field Type | Options/Settings |
|------------|------------|------------------|
| CRM Lead ID | Single line text | - |
| Title | Single line text | - |
| Email | Email | - |
| LinkedIn URL | URL | - |
| Company | Link to another record | Link to: Companies table, Allow linking to multiple records: NO |
| Status | Single select | Options: New, Enriched, Contacted, Responded, Dead |
| Enrichment Status | Single select | Options: Not Enriched, In Progress, Enriched, Failed |
| Enrichment Confidence | Single select | Options: High, Medium, Low |
| Intelligence Notes | Long text | - |
| Last Contacted | Date | - |
| Date Added | Created time | - |
| Last Modified | Last modified time | - |

**Create Views:**
1. "Need Enrichment"
   - Filter: Enrichment Status = "Not Enriched"
2. "By Company"
   - Group by: Company
   - Sort by: Company (A→Z)

---

### Table 3: INTELLIGENCE LOG

**Primary Field:** Summary (Long text)

**Fields to Add:**

| Field Name | Field Type | Options/Settings |
|------------|------------|------------------|
| Date | Date | Include time |
| Record Type | Single select | Options: Lead, Company |
| Lead | Link to another record | Link to: Leads table |
| Company | Link to another record | Link to: Companies table |
| Intelligence Type | Single select | Options: News, Funding, Pipeline Update, LinkedIn Activity, Conference, Hire, Partnership, Other |
| Source URL | URL | - |
| Confidence Level | Single select | Options: High, Medium, Low |
| Created | Created time | - |

**Create a View:**
- Name: "Recent Intelligence"
- Sort: Date (newest first)

---

### Table 4: ENRICHMENT QUEUE

**Primary Field:** Queue ID (Autonumber)

**Fields to Add:**

| Field Name | Field Type | Options/Settings |
|------------|------------|------------------|
| Record Type | Single select | Options: Lead, Company |
| Lead | Link to another record | Link to: Leads table |
| Company | Link to another record | Link to: Companies table |
| Fields to Enrich | Multiple select | Options: Email, Title, LinkedIn, Website, Focus Area, Funding, Pipeline, All |
| Priority | Single select | Options: High, Medium, Low |
| Status | Single select | Options: Queued, Processing, Complete, Failed |
| Date Requested | Created time | - |
| Date Completed | Date | - |
| Results | Long text | (will store JSON) |

**Create a View:**
- Name: "Pending"
- Filter: Status = "Queued" OR Status = "Processing"
- Sort: Priority (High→Low), Date Requested (oldest first)

---

## Step 3: Set Up Linked Record Fields

When you create the "Link to another record" fields, Airtable will ask if you want to create a linked field in the other table. **Say YES** for all of these:

1. **Leads → Company**: Creates "Leads" field in Companies table (shows all leads for that company)
2. **Intelligence Log → Lead**: Creates "Intelligence Log" field in Leads table
3. **Intelligence Log → Company**: Creates "Intelligence Log" field in Companies table
4. **Enrichment Queue → Lead**: Creates "Enrichment Queue" field in Leads table
5. **Enrichment Queue → Company**: Creates "Enrichment Queue" field in Companies table

---

## Step 4: Get Your API Credentials

You'll need these for the Python scripts:

1. Go to https://airtable.com/create/tokens
2. Click "Create new token"
3. Name it: "Lead Intelligence System"
4. Add these scopes:
   - `data.records:read`
   - `data.records:write`
   - `schema.bases:read`
5. Add access to your "Lead Intelligence System" base
6. Click "Create token"
7. **Copy and save the token** (you won't see it again!)

8. Get your Base ID:
   - Go to https://airtable.com/api
   - Click on your "Lead Intelligence System" base
   - The Base ID is in the URL: `https://airtable.com/[BASE_ID]/api/docs`
   - It starts with "app" (e.g., `appXXXXXXXXXXXXXX`)

---

## Step 5: Sample Data (Optional)

To test the structure, add a few sample records:

**Companies Table:**
- Name: "Acme Biologics"
- Location/HQ: "Zurich, Switzerland"
- Focus Area: Bispecifics
- Funding Stage: Series B
- Enrichment Status: Not Enriched

**Leads Table:**
- Name: "John Smith"
- Title: "VP Operations"
- Company: [Link to Acme Biologics]
- Status: New
- Enrichment Status: Not Enriched

---

## Next Steps

Once your Airtable base is set up:

1. Save your API token and Base ID
2. We'll create a `config.yaml` file with these credentials
3. Build the import script to bulk upload your 3-4K leads
4. Build the enrichment scripts

---

## Estimated Setup Time

- Creating tables and fields: 15-20 minutes
- Setting up API access: 5 minutes
- Adding sample data: 5 minutes

**Total: ~30 minutes**

---

## Tips

- **Use templates**: Once you have one field configured (e.g., a Single Select), you can duplicate it and just rename
- **Field order**: You can drag fields to reorder them after creation
- **Views are powerful**: Create custom views for different workflows (e.g., "High Priority Leads", "Recently Funded Companies")
- **Don't worry about perfection**: We can always add/modify fields later as we learn what works

---

## Questions or Issues?

Common issues:
- **Can't create linked fields**: Make sure the target table exists first
- **Missing field types**: Make sure you're using the correct field type names (they're case-sensitive)
- **API token doesn't work**: Make sure you added the correct scopes and base access

Let me know once you've completed the setup and we'll move on to building the scripts!
