import os
import keyring
import sys
import argparse
from dotenv import load_dotenv

def get_credentials():
    """
    Fetches database credentials from the Windows Credential Manager.

    This script reads the CREDENTIAL_TARGET from the .env file,
    uses the keyring library to look up the corresponding generic credential,
    and prints the username and password to stdout in a format that can be
    parsed by a batch script (KEY==VALUE).
    """
    try:
        # The batch script provides the path to the .env file.
        parser = argparse.ArgumentParser()
        parser.add_argument("--dotenv-path", required=True, help="Path to the .env file")
        parser.add_argument("--debug", action="store_true", help="Enable debug logging to stderr.")
        args = parser.parse_args()

        load_dotenv(dotenv_path=args.dotenv_path)
        # Get the target name and strip any accidental whitespace from the .env file.
        credential_target = os.getenv("CREDENTIAL_TARGET", "").strip()

        if not credential_target:
            print("Error: CREDENTIAL_TARGET not found in the .env file.", file=sys.stderr)
            sys.exit(1)

        if args.debug:
            # --- Enhanced Debugging ---
            active_keyring = keyring.get_keyring()
            print(f"DEBUG: Using keyring backend: {active_keyring.__class__.__name__}", file=sys.stderr)
            print(f"DEBUG: Attempting to fetch credential for target: '{credential_target}'", file=sys.stderr)

        # Fetch the credential object from the system's keyring (Windows Credential Manager)
        credential = keyring.get_credential(credential_target, None)

        if not credential:
            print(f"Error: No credential found in Windows Credential Manager for target '{credential_target}'. Please double-check the name.", file=sys.stderr)
            sys.exit(1)

        if credential:
            # Use .strip() to remove any accidental leading/trailing whitespace
            # from the credentials stored in Windows Credential Manager.
            print(f"DB_USERNAME=={credential.username.strip()}")
            print(f"DB_PASSWORD=={credential.password.strip()}")
    except Exception as e:
        # Print the error to stderr so it can be seen for debugging,
        # and exit with a non-zero status code to signal failure to the batch script.
        print(f"Error in get_credentials.py: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    get_credentials()