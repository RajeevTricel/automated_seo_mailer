# automated-seo-mailer

Automates daily Google PageSpeed reports for the Tricel site groups and emails a formatted HTML summary.

## What it does

- Reads the Tricel site groups from `sites.js`
- Calls the Google PageSpeed Insights API for each URL
- Supports `mobile`, `desktop`, or both strategies
- Builds a color-coded HTML email plus plain-text fallback
- Sends the report through any SMTP provider supported by Nodemailer
- Runs on a schedule with GitHub Actions

## Files

- `index.js` - main report runner
- `sites.js` - grouped site list
- `.github/workflows/daily-report.yml` - daily scheduler
- `.env.example` - local config template

## Local setup

1. Create a new private GitHub repository.
2. Copy these files into the repository.
3. Run:
   ```bash
   npm install
   cp .env.example .env
   ```
4. Fill in `.env`.
5. Run a local preview without sending email:
   ```bash
   npm run dry-run
   ```
6. Send a real email:
   ```bash
   npm run report
   ```

## Recommended secrets

Add these repository **Secrets** in GitHub:

- `PAGESPEED_API_KEY`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_SECURE`
- `SMTP_USER`
- `SMTP_PASS`
- `EMAIL_FROM`
- `EMAIL_TO`
- `EMAIL_CC`
- `EMAIL_BCC`

Add these repository **Variables** in GitHub if you want to override defaults:

- `SUBJECT_PREFIX`
- `REPORT_STRATEGIES`
- `REQUEST_DELAY_MS`

## SMTP examples

### Gmail App Password
- `SMTP_HOST=smtp.gmail.com`
- `SMTP_PORT=465`
- `SMTP_SECURE=true`

### Office 365
- `SMTP_HOST=smtp.office365.com`
- `SMTP_PORT=587`
- `SMTP_SECURE=false`

### SendGrid SMTP
- `SMTP_HOST=smtp.sendgrid.net`
- `SMTP_PORT=587`
- `SMTP_SECURE=false`

## Notes

- The script waits between requests to reduce the chance of PageSpeed throttling.
- The scheduled workflow runs at **08:30 Europe/Dublin**.
- If you only want one strategy, set `REPORT_STRATEGIES=mobile` or `REPORT_STRATEGIES=desktop`.
