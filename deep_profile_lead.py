name: Deep Profile Leads

on:
  schedule:
    # Monthly on 1st at 5 AM UTC - high priority leads (ICP 75+)
    - cron: '0 5 1 * *'
    # Bi-annual on Jan 1 and Jul 1 at 4 AM UTC - medium priority leads (ICP 50-74)
    - cron: '0 4 1 1,7 *'
  
  workflow_dispatch:
    inputs:
      mode:
        description: 'Profiling mode'
        required: false
        default: 'Auto: All marked leads (checkbox)'
        type: choice
        options:
          - 'Auto: All marked leads (checkbox)'
          - 'Tier: Monthly (ICP 75+)'
          - 'Tier: Bi-annual (ICP 50-74)'
          - 'Tier: All priority (ICP 50+)'
          - 'Manual: Enter lead name'
      lead_name:
        description: 'Lead name (only for manual mode)'
        required: false
        type: string
      limit:
        description: 'Max number of leads to profile'
        required: false
        default: '20'
        type: string

jobs:
  profile:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
    
    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
    
    - name: Create config from secrets
      env:
        AIRTABLE_API_KEY: ${{ secrets.AIRTABLE_API_KEY }}
        AIRTABLE_BASE_ID: ${{ secrets.AIRTABLE_BASE_ID }}
        ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
      run: |
        python setup.py
    
    - name: Determine which tier to run
      id: tier
      run: |
        MODE="${{ github.event.inputs.mode || 'auto' }}"
        DAY_OF_MONTH=$(date +%-d)
        MONTH=$(date +%-m)
        
        # For scheduled runs, determine tier based on date
        if [ "$MODE" = "auto" ]; then
          if [ "$DAY_OF_MONTH" = "1" ]; then
            if [ "$MONTH" = "1" ] || [ "$MONTH" = "7" ]; then
              # Jan 1 or Jul 1 - run both tiers
              echo "run_monthly=true" >> $GITHUB_OUTPUT
              echo "run_biannual=true" >> $GITHUB_OUTPUT
            else
              # Other 1st of month - just monthly
              echo "run_monthly=true" >> $GITHUB_OUTPUT
              echo "run_biannual=false" >> $GITHUB_OUTPUT
            fi
          else
            echo "run_monthly=false" >> $GITHUB_OUTPUT
            echo "run_biannual=false" >> $GITHUB_OUTPUT
          fi
          echo "run_manual=false" >> $GITHUB_OUTPUT
          echo "run_auto=false" >> $GITHUB_OUTPUT
        elif [[ "$MODE" == *"Monthly"* ]]; then
          echo "run_monthly=true" >> $GITHUB_OUTPUT
          echo "run_biannual=false" >> $GITHUB_OUTPUT
          echo "run_manual=false" >> $GITHUB_OUTPUT
          echo "run_auto=false" >> $GITHUB_OUTPUT
        elif [[ "$MODE" == *"Bi-annual"* ]]; then
          echo "run_monthly=false" >> $GITHUB_OUTPUT
          echo "run_biannual=true" >> $GITHUB_OUTPUT
          echo "run_manual=false" >> $GITHUB_OUTPUT
          echo "run_auto=false" >> $GITHUB_OUTPUT
        elif [[ "$MODE" == *"All priority"* ]]; then
          echo "run_monthly=true" >> $GITHUB_OUTPUT
          echo "run_biannual=true" >> $GITHUB_OUTPUT
          echo "run_manual=false" >> $GITHUB_OUTPUT
          echo "run_auto=false" >> $GITHUB_OUTPUT
        elif [[ "$MODE" == *"Manual"* ]]; then
          echo "run_monthly=false" >> $GITHUB_OUTPUT
          echo "run_biannual=false" >> $GITHUB_OUTPUT
          echo "run_manual=true" >> $GITHUB_OUTPUT
          echo "run_auto=false" >> $GITHUB_OUTPUT
        else
          # Auto: All marked leads
          echo "run_monthly=false" >> $GITHUB_OUTPUT
          echo "run_biannual=false" >> $GITHUB_OUTPUT
          echo "run_manual=false" >> $GITHUB_OUTPUT
          echo "run_auto=true" >> $GITHUB_OUTPUT
        fi
    
    - name: "Manual: Profile specific lead"
      if: steps.tier.outputs.run_manual == 'true'
      env:
        AIRTABLE_API_KEY: ${{ secrets.AIRTABLE_API_KEY }}
        AIRTABLE_BASE_ID: ${{ secrets.AIRTABLE_BASE_ID }}
        ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
      run: |
        LEAD_NAME="${{ github.event.inputs.lead_name }}"
        if [ -z "$LEAD_NAME" ]; then
          echo "Error: Lead name required for manual mode"
          exit 1
        fi
        echo "Manual mode: Profiling $LEAD_NAME"
        python deep_profile_lead.py --lead "$LEAD_NAME"
    
    - name: "Auto: Profile all marked leads"
      if: steps.tier.outputs.run_auto == 'true'
      env:
        AIRTABLE_API_KEY: ${{ secrets.AIRTABLE_API_KEY }}
        AIRTABLE_BASE_ID: ${{ secrets.AIRTABLE_BASE_ID }}
        ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
      run: |
        LIMIT="${{ github.event.inputs.limit || '20' }}"
        echo "Auto mode: Profiling all leads marked with 'Deep Profile' checkbox"
        python deep_profile_lead.py --auto --limit $LIMIT
    
    - name: "Tier: Monthly High-Priority (ICP 75+)"
      if: steps.tier.outputs.run_monthly == 'true'
      env:
        AIRTABLE_API_KEY: ${{ secrets.AIRTABLE_API_KEY }}
        AIRTABLE_BASE_ID: ${{ secrets.AIRTABLE_BASE_ID }}
        ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
      run: |
        echo "=============================================="
        echo "MONTHLY: Deep profiling ICP 75+ leads"
        echo "Refresh period: 30 days"
        echo "No limit - processing all qualifying leads"
        echo "=============================================="
        python deep_profile_lead.py --tier monthly
    
    - name: "Tier: Bi-Annual Medium-Priority (ICP 50-74)"
      if: steps.tier.outputs.run_biannual == 'true'
      env:
        AIRTABLE_API_KEY: ${{ secrets.AIRTABLE_API_KEY }}
        AIRTABLE_BASE_ID: ${{ secrets.AIRTABLE_BASE_ID }}
        ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
      run: |
        echo "=============================================="
        echo "BI-ANNUAL: Deep profiling ICP 50-74 leads"
        echo "Refresh period: 180 days"
        echo "No limit - processing all qualifying leads"
        echo "=============================================="
        python deep_profile_lead.py --tier biannual
    
    - name: Upload profile log
      if: always()
      uses: actions/upload-artifact@v4
      with:
        name: deep-profile-log-${{ github.run_id }}
        path: deep_profile.log
        retention-days: 30
