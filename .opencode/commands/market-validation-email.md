---
description: Manage email outreach queue
---

Manage email outreach with review queue.

**Usage:**
```
market-validation email prep --to <email> --subject "<subject>" --body "<body>" --company "<name>"
market-validation email list
market-validation email approve <id>
market-validation email delete <id>
market-validation email send <id>
```

**Workflow:**

1. **Prep** - Queue email for review (doesn't send immediately)
   ```
   market-validation email prep \
     --to contact@company.com \
     --subject "Partnership Opportunity" \
     --body "Hi, I'd like to discuss..." \
     --company "Company Name"
   ```

2. **Review** - Check in dashboard at http://localhost:8787

3. **Approve** - Send the email
   ```
   market-validation email approve <email_id>
   ```

**Setup:** Requires SMTP credentials in `.env` file.
