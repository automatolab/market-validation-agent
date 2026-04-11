---
description: Start the market research dashboard
---

Start the interactive dashboard server or generate static HTML.

**Usage:**
```
market-validation dashboard
market-validation dashboard --static
market-validation dashboard --port 9000
```

**Options:**
- `--static` - Generate static HTML instead of running server
- `--port <port>` - Custom port (default: 8787)
- `--host <host>` - Custom host (default: 127.0.0.1)
- `--no-open` - Don't open browser automatically

**Features:**
- Project selector with URL persistence
- Inline company row editing (Edit Row → Save/Cancel/Delete)
- Email queue management
- KPI dashboard

**URL:** http://localhost:8787 (or custom port)
