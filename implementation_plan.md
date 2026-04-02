# Automate Tricel PageSpeed Reports

This plan outlines the steps to transition the PageSpeed monitoring from a manual, client-side HTML file to a fully automated server-side process using Node.js and GitHub Actions.

## User Review Required

> [!IMPORTANT]
> To send emails automatically, we need an SMTP server (an email sending service). You have a few options, and I need to know which one you prefer before writing the code:
> 1. **Gmail App Password (Easiest)**: If you have a Google/Gmail account, you can generate an "App Password" which gives our script permission to send emails from your address.
> 2. **SendGrid (Most professional)**: You can create a free account on SendGrid.com, which gives you an API key specifically meant for scripts to send automated emails.
> 3. **Office 365 / Outlook**: Requires setting up an App Password within your Microsoft account.
> 
> *Which method would you like to use for sending the emails?*

## Proposed Changes

We will create a new directory for this project to keep it clean. Let's call it `automated-seo-mailer`. Inside, we will generate the following structure:

### Node.js Automation App

#### [NEW] `package.json`
Will define the project dependencies. We will need:
- `nodemailer`: To construct and send the email.

#### [NEW] `index.js`
This will be the core script. It will:
1. Loop over your existing `SITE_GROUPS`.
2. Fetch scores from the Google PageSpeed Insights API (using your API key).
3. Introduce a 5-10 second delay between requests to avoid getting blocked by Google.
4. Construct a clean, modern HTML email summarizing the scores using tables and color-coding (Green/Yellow/Red).
5. Send the email using `nodemailer`.

### GitHub Actions Auto-Scheduler

#### [NEW] `.github/workflows/daily-report.yml`
This is the configuration file for the cloud server. It will tell GitHub to:
1. Boot up a machine every morning at 8:30 AM (or slightly before).
2. Install the Node.js dependencies.
3. Run the `index.js` script to generate and perform the email sending.

## Open Questions

> [!WARNING]
> **API Key Security**
> Your PageSpeed API Key is currently hardcoded in the HTML. For security, we should store both the API Key and the Email Password/Key as "Secrets" in GitHub, so they are never public. Are you comfortable with me setting the code up to read these from Environment Variables (`process.env.API_KEY`), which you will later input into GitHub settings?

## Verification Plan

### Automated Tests
- We will do a manual trigger of the Node script locally to ensure the PageSpeed API works and doesn't get rate-limited.
- We will send a test email to your own email address first to verify the layout and colors look good before we set it to send to `mkteam@tricel.ie`.

### Manual Verification
- You will need to create a private GitHub repository, upload these files, input your secrets, and verify the GitHub Action runs successfully at the scheduled time. I will provide step-by-step instructions for this.
