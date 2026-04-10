"""
Generate credentials/token.json for Google Drive uploads.

Run ONCE from the project root (outside Docker):
    python3 scripts/generate_token.py

Prerequisites:
    1. Create an OAuth 2.0 Client ID (Desktop app) in Google Cloud Console.
    2. Enable the Google Drive API for your project.
    3. Download the JSON and save it as: credentials/oauth_credentials.json
    4. pip install google-auth-oauthlib
"""

import json
import pathlib
import sys

CREDENTIALS_FILE = pathlib.Path("credentials/oauth_credentials.json")
TOKEN_FILE       = pathlib.Path("credentials/token.json")
SCOPES           = ["https://www.googleapis.com/auth/drive"]


def main() -> None:
    if not CREDENTIALS_FILE.exists():
        print(f"ERROR: {CREDENTIALS_FILE} not found.")
        print("Download the OAuth 2.0 Client ID JSON from Google Cloud Console")
        print("and save it as credentials/oauth_credentials.json")
        sys.exit(1)

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("Missing dependency: pip install google-auth-oauthlib")
        sys.exit(1)

    flow  = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), scopes=SCOPES)
    creds = flow.run_local_server(port=0)

    TOKEN_FILE.write_text(json.dumps({
        "token":         creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri":     creds.token_uri,
        "client_id":     creds.client_id,
        "client_secret": creds.client_secret,
        "scopes":        list(creds.scopes),
    }, indent=2))

    print(f"token.json written to {TOKEN_FILE}")
    print("The token refreshes automatically when it expires.")


if __name__ == "__main__":
    main()
