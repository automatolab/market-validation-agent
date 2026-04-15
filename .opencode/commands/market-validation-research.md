---
description: Manage research projects
---

Manage research projects in the database.

**Usage:**
```
market-validation research list
market-validation research get <id>
market-validation research create --name "<name>" --market "<market>" [--product "<product>"] --geography "<geography>"
market-validation research export <id> [--output <file>]
```

**Examples:**

```bash
# List all research projects
market-validation research list

# Get research details
market-validation research get abc123

# Create new research
market-validation research create --name "My Research" --market "BBQ" --product "brisket" --geography "San Jose, CA"

# Export to markdown
market-validation research export abc123 --output report.md
```

**Database:** `output/market-research.sqlite3`
