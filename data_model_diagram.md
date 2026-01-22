# Lead Intelligence System - Data Model & Relationships

## Table Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         COMPANIES TABLE                          │
├─────────────────────────────────────────────────────────────────┤
│ • Company Name (Primary)                                         │
│ • Website, LinkedIn, Location                                    │
│ • Focus Area, Technology Platform                                │
│ • Funding Stage, Total Funding, Latest Round                     │
│ • Pipeline Stage, Lead Programs, Therapeutic Areas               │
│ • Current CDMO, Manufacturing Status                             │
│ • Enrichment Status, ICP Fit Score, Urgency Score                │
│ • Intelligence Notes, Last Intelligence Check                    │
│                                                                   │
│ ◄── Linked from: Leads (many-to-one)                            │
│ ◄── Linked from: Intelligence Log                               │
│ ◄── Linked from: Enrichment Queue                               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ One Company
                              │ Multiple Leads
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                           LEADS TABLE                            │
├─────────────────────────────────────────────────────────────────┤
│ • Lead Name (Primary)                                            │
│ • CRM Lead ID (for syncing back to your CRM)                     │
│ • Title, Email, LinkedIn URL                                     │
│ • Company ──► Links to Companies Table                          │
│ • Status (New/Enriched/Contacted/etc)                           │
│ • Enrichment Status, Enrichment Confidence                       │
│ • Intelligence Notes, Last Contacted                             │
│                                                                   │
│ ◄── Linked from: Intelligence Log                               │
│ ◄── Linked from: Enrichment Queue                               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      INTELLIGENCE LOG                            │
├─────────────────────────────────────────────────────────────────┤
│ • Summary (Primary)                                              │
│ • Date, Record Type                                              │
│ • Lead ──► Links to Leads Table                                 │
│ • Company ──► Links to Companies Table                          │
│ • Intelligence Type (News/Funding/LinkedIn/etc)                  │
│ • Source URL, Confidence Level                                   │
│                                                                   │
│ Purpose: Track all intelligence gathered over time               │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                       ENRICHMENT QUEUE                           │
├─────────────────────────────────────────────────────────────────┤
│ • Queue ID (Autonumber)                                          │
│ • Record Type, Priority                                          │
│ • Lead ──► Links to Leads Table                                 │
│ • Company ──► Links to Companies Table                          │
│ • Fields to Enrich, Status                                       │
│ • Date Requested, Date Completed, Results                        │
│                                                                   │
│ Purpose: Track enrichment jobs and results                       │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow Example

### When You Add New Leads:

```
1. You have CSV:
   ┌──────────────────────────────────────────┐
   │ John Smith, VP Ops, Acme Biologics       │
   │ Jane Doe, CSO, Acme Biologics            │
   │ Bob Lee, Head CMC, BetaPharma            │
   └──────────────────────────────────────────┘
                    │
                    ▼
   
2. Import script runs:
   
   COMPANIES TABLE:
   ┌──────────────────────────────────────────┐
   │ Acme Biologics                           │
   │ Status: Not Enriched                     │
   ├──────────────────────────────────────────┤
   │ BetaPharma                               │
   │ Status: Not Enriched                     │
   └──────────────────────────────────────────┘
   
   LEADS TABLE:
   ┌──────────────────────────────────────────┐
   │ John Smith → [Acme Biologics]            │
   │ Status: Not Enriched                     │
   ├──────────────────────────────────────────┤
   │ Jane Doe → [Acme Biologics]              │
   │ Status: Not Enriched                     │
   ├──────────────────────────────────────────┤
   │ Bob Lee → [BetaPharma]                   │
   │ Status: Not Enriched                     │
   └──────────────────────────────────────────┘
```

### When You Run Enrichment:

```
3. Run: python enrich_companies.py --status "Not Enriched"

   COMPANIES TABLE (After):
   ┌────────────────────────────────────────────────────────┐
   │ Acme Biologics                                         │
   │ ✓ Website: acmebiologics.com                          │
   │ ✓ Focus Area: Bispecifics                             │
   │ ✓ Funding: Series B, $75M                             │
   │ ✓ Pipeline: Phase 1/2                                 │
   │ ✓ ICP Fit Score: 85                                   │
   │ ✓ Urgency Score: 70                                   │
   │ Status: Enriched                                       │
   └────────────────────────────────────────────────────────┘

   INTELLIGENCE LOG (New entries):
   ┌────────────────────────────────────────────────────────┐
   │ 2025-01-22: Acme Biologics raised $75M Series B        │
   │ Source: PR Newswire                                    │
   │ Type: Funding                                          │
   └────────────────────────────────────────────────────────┘
```

```
4. Run: python enrich_leads.py --status "Not Enriched"

   LEADS TABLE (After):
   ┌────────────────────────────────────────────────────────┐
   │ John Smith → [Acme Biologics]                          │
   │ ✓ Title: VP Operations (confirmed)                    │
   │ ✓ Email: john.smith@acmebiologics.com                 │
   │ ✓ LinkedIn: linkedin.com/in/johnsmith                 │
   │ Status: Enriched                                       │
   │ Confidence: High                                       │
   └────────────────────────────────────────────────────────┘
```

## Key Benefits of This Structure

### 1. No Duplication
```
❌ BAD (without linking):
Leads Table:
- John Smith, Acme Biologics, Series B, mAbs, ...
- Jane Doe, Acme Biologics, Series B, mAbs, ...
→ Company data duplicated 2x!

✓ GOOD (with linking):
Companies Table:
- Acme Biologics: Series B, mAbs, ... (stored once)

Leads Table:
- John Smith → [link to Acme Biologics]
- Jane Doe → [link to Acme Biologics]
→ Company data stored 1x, referenced 2x!
```

### 2. Efficient Enrichment
```
Without linking:
- Enrich John Smith's company info
- Enrich Jane Doe's company info (duplicate work!)
Cost: 2x enrichment API calls

With linking:
- Enrich Acme Biologics once
- Both leads automatically reference updated data
Cost: 1x enrichment API call
```

### 3. Easy Analysis
```
In Airtable, you can create views like:

"Leads by Company":
Acme Biologics (ICP Score: 85)
  ├─ John Smith (VP Operations)
  ├─ Jane Doe (CSO)
  └─ Mike Chen (Head CMC)

BetaPharma (ICP Score: 92)
  └─ Bob Lee (Head CMC)

→ Instantly see which companies have multiple decision makers!
```

### 4. Smart Prioritization
```
Query: "Show me all leads at Series B+ companies 
        with ICP Score > 80 in bispecifics"

Result: Because leads link to companies,
        you can filter on BOTH lead and company attributes

→ John Smith (Acme Biologics - Series B, Bispecifics, Score 85)
→ Jane Doe (Acme Biologics - Series B, Bispecifics, Score 85)
```

## Airtable Views You'll Love

Once set up, create these useful views:

### In LEADS table:
1. **"Hot Leads"**: Company ICP Score > 80, Status = "New"
2. **"By Company"**: Grouped by Company, sorted by ICP Score
3. **"Need Follow-up"**: Status = "Contacted", Last Contacted > 2 weeks ago
4. **"Multiple Decision Makers"**: Companies with 2+ linked leads

### In COMPANIES table:
1. **"Top Prospects"**: ICP Score > 80, Urgency Score > 70
2. **"Series B+ Bispecifics"**: Funding Stage = Series B/C/D+, Focus = Bispecifics
3. **"Recently Funded"**: Latest Funding Round within last 6 months
4. **"No CDMO Partner"**: Manufacturing Status = "No Public Partner"

## Next: Setting Up Airtable

Follow the setup guide to create your base, then we'll build:
1. `import_leads.py` - Bulk upload your 3-4K leads
2. `enrich_companies.py` - Enrich company intelligence
3. `enrich_leads.py` - Enrich lead contact info
4. `config.yaml` - Store your API keys

Ready to start? Complete the Airtable setup and let me know!
