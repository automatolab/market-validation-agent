---
description: Generate call sheets from research data
---

Generate call sheets for outreach.

**Usage:**
```
market-validation call-sheet [--status <status>] [--limit <n>]
market-validation call-sheet --research-id <id> [--status <status>]
```

**Examples:**

```bash
# Get all qualified leads
market-validation call-sheet --status qualified

# Limit to top 20
market-validation call-sheet --status qualified --limit 20

# Specific research
market-validation call-sheet --research-id abc123 --status qualified
```

**Status Options:**
- `new` - Discovered but not qualified
- `qualified` - Ready for outreach
- `contacted` - Already reached out
- `interested` - Responded positively
- `not_interested` - Declined

**Output:** Markdown table with company, phone, email, notes
