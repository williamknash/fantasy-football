# GitHub Secrets Setup for Automated Scoring

To enable the GitHub Actions workflow to run the scoring job automatically, you need to add the following secrets to your repository.

## Where to Add Secrets

Go to: **https://github.com/timmygiants/fantasy-football/settings/secrets/actions**

Click **"New repository secret"** for each of the following:

## Required Secrets

Get these values from your `.streamlit/secrets.toml` file:

### RapidAPI Configuration
- **`RAPIDAPI_KEY`** - Your RapidAPI key
  - From: `[rapidapi]` section, `key` field

### Google Sheets Configuration
- **`SPREADSHEET_URL`** - Your Google Sheets URL
  - From: `[connections.gsheets]` section, `spreadsheet` field

### Google Cloud Platform (GCP) Service Account
All from `[connections.gsheets]` section:

- **`GCP_TYPE`** - Usually `service_account`
  - Field: `type`

- **`GCP_PROJECT_ID`** - Your GCP project ID
  - Field: `project_id`

- **`GCP_PRIVATE_KEY_ID`** - Private key ID
  - Field: `private_key_id`

- **`GCP_PRIVATE_KEY`** - The full private key (including the BEGIN/END lines)
  - Field: `private_key`
  - ⚠️ Copy the entire multi-line key including `-----BEGIN PRIVATE KEY-----` and `-----END PRIVATE KEY-----`

- **`GCP_CLIENT_EMAIL`** - Service account email
  - Field: `client_email`

- **`GCP_CLIENT_ID`** - Client ID
  - Field: `client_id`

- **`GCP_AUTH_URI`** - OAuth auth URI (usually `https://accounts.google.com/o/oauth2/auth`)
  - Field: `auth_uri`

- **`GCP_TOKEN_URI`** - Token URI (usually `https://oauth2.googleapis.com/token`)
  - Field: `token_uri`

- **`GCP_AUTH_PROVIDER_CERT_URL`** - Auth provider cert URL (usually `https://www.googleapis.com/oauth2/v1/certs`)
  - Field: `auth_provider_x509_cert_url`

- **`GCP_CLIENT_CERT_URL`** - Client cert URL
  - Field: `client_x509_cert_url`

## Testing

Once all secrets are added:
1. Go to the **Actions** tab in your repository
2. Select **"Fetch NFL Scores"** workflow
3. Click **"Run workflow"** to manually test
4. Check the logs to ensure it runs successfully

## Troubleshooting

If you see errors about missing secrets:
- Verify each secret name matches exactly (case-sensitive)
- For `GCP_PRIVATE_KEY`, make sure you copied the entire key including newlines
- Check that the spreadsheet URL is correct and the service account has access to it
