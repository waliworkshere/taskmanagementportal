# Task Portal

A ticketing and progress-tracking portal for the real-time data team. The team logs and works tickets; managers follow progress, raise requests and sign off finished work. Every report prints cleanly to A4 or saves as a PDF.

**Live:** https://waliworkshere.github.io/taskmanagementportal/

## What it does

| Area | Details |
|---|---|
| Two kinds of login | **Team** (admin and team members) work tickets and log time. **Managers** see everything, raise requests, comment, change priorities and due dates, and sign off completed work. |
| Tickets | Key (TSK-0001…), type, priority P1–P4, status, system, data layer, assignee, due date, estimate, blocked reason. |
| Ticket types | CDC table onboarding, Kafka topic / stream, Informatica mapping / workflow, Incident / job failure, Data quality / reconciliation, Report / MicroStrategy request, Access / environment, Documentation / runbook, Learning / KT session, Enhancement / change request, Admin / other. |
| Workflow | New → In progress → Blocked / In review → Done → manager sign-off. Moving to Blocked asks for a reason. |
| Board | Drag cards between status columns. |
| Comments, work log, history | Every ticket has a discussion thread, a time log (hours per day) and an automatic history of status, assignee, priority, due date and sign-off changes. |
| Dashboard | Open, in progress, blocked, overdue, done this week, hours this week, your tickets, items waiting for sign-off, recent activity. |
| Reports | Pick any period and person. Completed work (with sign-off status), in progress, blocked, overdue, due next 7 days, and time logged, with a signature block. **Print / Save as PDF** gives an A4 document. |
| Export | Ticket list and time log as CSV for Excel. |

## Accounts

- New people click **Request an account** on the sign-in page. Their account stays *Pending* until an admin approves it on the **Team** page and gives it a role.
- Roles: **Admin (team)**, **Team member**, **Manager**, **Pending approval**.
- Only a **Manager** can sign off work, so nobody approves their own tickets.

## How it's built

- Static site: `index.html`, `styles.css`, `app.js`, `config.js`. No build step; hosted on GitHub Pages.
- Database and logins: [Supabase](https://supabase.com) (PostgreSQL + Auth). Schema in [`supabase/schema.sql`](supabase/schema.sql).
- Security is enforced in the database with row-level security: pending accounts see nothing, only admins change roles or delete tickets, only managers sign off, and history can't be edited. The key in `config.js` is the public "publishable" key, which is safe to expose.

## Run locally

```bash
python3 -m http.server 8000
# open http://localhost:8000
```

## One-time Supabase setting

In the Supabase dashboard → **Authentication → URL Configuration**, set **Site URL** to `https://waliworkshere.github.io/taskmanagementportal/` so sign-up confirmation emails link back to the portal.
