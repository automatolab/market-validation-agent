---
description: Run the market research 3-step pipeline (find/qualify/enrich)
---

Run the market research 3-step pipeline using the Agent class.

**Usage:**
```
market-validation run "<market>" "<geography>" [--product "<product>"]
market-validation run --research-id <id> find|qualify|enrich [...]
market-validation run --help
```

**Pipeline Steps:**

1. **find** - Discover companies via web search
   ```
   market-validation run --research-id <id> find "<market>" "<geography>" [--product "<product>"]
   ```

2. **qualify** - AI assessment + volume estimation
   ```
   market-validation run --research-id <id> qualify
   ```

3. **enrich** - Find contacts via 8 sources
   ```
   market-validation run --research-id <id> enrich "<company_name>" [--location "<location>"]
   ```

**Examples:**

```bash
# Full pipeline in one command
market-validation run "BBQ restaurants" "San Jose, CA" --product "brisket"

# Run steps separately
market-validation run "BBQ" "San Jose" --product brisket find
market-validation run --research-id abc123 qualify
market-validation run --research-id abc123 enrich "Restaurant Name"
```

**Output:**
- Companies added to database
- Qualification scores assigned
- Contact info enriched (email, phone)
