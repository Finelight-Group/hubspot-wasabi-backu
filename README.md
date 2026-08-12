# HubSpot → Wasabi daily backup

Replaces HubSpot's native Backup & Restore (max once a week) with a script
that pulls CRM data via the HubSpot API every day and stores it in Wasabi
instead of SharePoint.

## What it backs up

- All standard CRM objects: contacts, companies, deals, tickets, products,
  line items, quotes, calls, emails, meetings, notes, tasks.
- Every custom object type visible to the private app token (advisory
  boards, assignments, surveys, awards, brands, events, Hapily
  events/registrants/sessions, etc.) — discovered automatically each run,
  so new custom objects get picked up without touching the script.

**Not included in v1** (flagging so it's a known gap, not a silent one):
- Associations between records (e.g. which contacts belong to which deal).
  Records themselves are fully backed up; the relationships between them
  are not yet.
- Marketing assets — workflows, email templates, landing pages, forms.
  HubSpot's native backup doesn't cover these either, so this isn't a
  regression, but worth knowing if you assumed it was in scope.

## One-time setup

### 1. Create a HubSpot private app

In HubSpot: Settings → Integrations → Private Apps → Create a private app.

Grant read scopes for:
- `crm.objects.contacts.read`
- `crm.objects.companies.read`
- `crm.objects.deals.read`
- `crm.objects.tickets.read`
- `crm.objects.line_items.read`
- `crm.objects.products.read`
- `crm.objects.quotes.read`
- `crm.schemas.custom.read`
- `crm.objects.custom.read` (covers custom object records — HubSpot may
  list these per-object depending on your account; grant read access to
  each custom object type you want backed up)

Copy the generated access token — this is `HUBSPOT_PRIVATE_APP_TOKEN`.

If a custom object doesn't show up in a backup run, it's almost always
because this token wasn't granted read access to it — check the private
app's scopes first.

### 2. Create a Wasabi bucket

In the Wasabi console, create a bucket (e.g. `finelight-hubspot-backups`)
in whichever region you prefer, and create an access key pair under
Access Keys. You'll need:
- `WASABI_ACCESS_KEY`
- `WASABI_SECRET_KEY`
- `WASABI_BUCKET` (the bucket name)
- `WASABI_REGION` (e.g. `us-east-1`)

Optional but recommended: set a lifecycle rule on the bucket to expire
objects older than however long you want to retain backups (e.g. 90 days),
so this doesn't quietly grow into the same problem you had with SharePoint.

### 3. Add the secrets to GitHub

In this repo: Settings → Secrets and variables → Actions → New repository
secret. Add all five:
- `HUBSPOT_PRIVATE_APP_TOKEN`
- `WASABI_ACCESS_KEY`
- `WASABI_SECRET_KEY`
- `WASABI_BUCKET`
- `WASABI_REGION`

### 4. Test it

First, test locally if you can (recommended before relying on a scheduled
run):

```bash
pip install -r requirements.txt
export HUBSPOT_PRIVATE_APP_TOKEN=...
export WASABI_ACCESS_KEY=...
export WASABI_SECRET_KEY=...
export WASABI_BUCKET=...
export WASABI_REGION=us-east-1
export FULL_SYNC=true   # first run should always be a full export
python backup_hubspot_to_wasabi.py
```

Check the Wasabi bucket for `hubspot-backups/<date>/hubspot-backup-<date>.zip`.

Then in GitHub: Actions tab → "HubSpot Daily Backup to Wasabi" → Run
workflow → tick `full_sync` for the first manual run. After that, the
`0 2 * * *` schedule takes over and runs incrementally every day
automatically.

## How incremental sync works

The first run (or any run with `FULL_SYNC=true`) exports every record of
every object type. Every run after that only pulls records whose
`hs_lastmodifieddate` is on or after the last successful run — tracked via
a small state file the script keeps in the same Wasabi bucket
(`hubspot-backups/_state/last_run.json`). This keeps daily runs fast and
well within HubSpot's API rate limits regardless of portal size.

The watermark only advances after a run's upload succeeds, so a failed run
doesn't lose track of what still needs to be backed up — the next run
just picks up from the last known-good point.

## Failure alerts

GitHub sends an email to the repo's watchers by default when a scheduled
Actions workflow fails, which covers the same "something went wrong"
signal HubSpot's native backup gives you. If you want it routed to Slack
or somewhere more visible, that's a small addition to the workflow file
(happy to add it if wanted).

## Restoring from a backup

Each zip in `hubspot-backups/<date>/` contains one JSON file per object
type (raw HubSpot API records, including all properties) plus a
`_manifest.json` summarizing what was exported and how many records. There
is no automated "restore into HubSpot" step — that would need to be built
separately using HubSpot's create/update API endpoints, the same as any
DIY backup approach.
