# sharepoint.py
# Microsoft Graph API integration for SharePoint file operations

import json
import requests
import os
from pathlib import Path
from O365 import Account
from config import (
    APPLICATION_CLIENT_ID,
    CLIENT_SECRET_VALUE,
    DIRECTORY_TENANT_ID,
    SITE_HOSTNAME,
    SITE_PATH,
    DEV_ROOT_FOLDER_ID,
    PROD_ROOT_FOLDER_ID
)


class SharePoint:
    def __init__(self):
        self.main_endpoint = "https://graph.microsoft.com/v1.0"
        self.host_name = SITE_HOSTNAME
        self.tenant_id = DIRECTORY_TENANT_ID
        self.client_id = APPLICATION_CLIENT_ID
        self.client_secret = CLIENT_SECRET_VALUE
        self.expiration_datetime = None
        self._site_url = "%s/sites/%s:/sites/%s"  # main_endpoint, host_name, site_path
        self.site_id = ""
        self.access_token = ""
        self.drive_id = ""

        self.authenticate_and_get_drive_id()

    def authenticate_and_get_drive_id(self):
        """This method authenticates the O365 account and gets the drive id."""
        self.access_token = self.authenticate_account()
        self.site_id = self.get_site_id(self.access_token)
        drive_info = self.get_site_drive_info(self.site_id, self.access_token)
        self.drive_id = drive_info["id"]

    def authenticate_account(self):
        """The function authenticates an O365 account using Microsoft Graph and returns the access token.

        Return:
            the access token obtained from the authentication process.
        """
        credentials = (self.client_id, self.client_secret)
        try:
            account = Account(credentials, auth_flow_type="credentials", tenant_id=self.tenant_id)
            account.authenticate()
            self.expiration_datetime = account.connection.token_backend.token.expiration_datetime
            with open("o365_token.txt") as f:
                data = f.read()
                js = json.loads(data)
                access_token = js["access_token"]
                print("Authentication successful!")
                return access_token
        except Exception as ex:
            print(f"Authentication failed: {ex}")
            raise ex

    def get_site_id(self, access_token):
        """The function retrieves the site ID for a given host name using an access token.

        :param access_token: The access_token parameter is a token that is used to authenticate the user
        and authorize access to the API. It is typically obtained by the user through an authentication
        process
        :return: the site ID.
        """
        try:
            result = requests.get(
                self._site_url % (self.main_endpoint, self.host_name, SITE_PATH),
                headers={"Authorization": "Bearer " + access_token},
            )
            site_info = result.json()
            site_id = site_info["id"]
            print(f"Site ID retrieved: {site_id}")
            return site_id
        except Exception as ex:
            print(f"Getting Site ID Failed: {ex}")
            raise ex

    def get_site_drive_info(self, site_id, access_token):
        """The function retrieves drive information for a specific site using the site ID and access token.

        :param site_id: The site_id parameter is the unique identifier for a specific site or location.
        It is used to specify which site's drive information should be retrieved
        :param access_token: The access_token parameter is a token that is used to authenticate the user
        and authorize access to the Microsoft Graph API. It is typically obtained by the user logging in
        and granting permission to the application to access their data
        :return: the drive information for a specific site.
        """
        try:
            result = requests.get(
                f"{self.main_endpoint}/sites/{site_id}/drive", headers={"Authorization": "Bearer " + access_token}
            )
            drive_info = result.json()
            print(f"Drive ID retrieved: {drive_info['id']}")
            return drive_info
        except Exception as ex:
            print(f"Getting Drive Info Failed: {ex}")
            raise ex

    def get_files_in_documents(self):
        """Get files from the Documents library"""
        try:
            result = requests.get(
                f"{self.main_endpoint}/drives/{self.drive_id}/root/children",
                headers={"Authorization": "Bearer " + self.access_token}
            )
            files_data = result.json()
            files = files_data.get('value', [])

            print(f"\nFound {len(files)} items in Documents library:")
            print("-" * 80)

            for file_item in files:
                file_type = 'FOLDER' if 'folder' in file_item else 'FILE'
                size_mb = file_item.get('size', 0) / (1024*1024) if file_item.get('size') else 0

                print(f"[{file_type}] {file_item.get('name')}")
                print(f"    ID: {file_item.get('id')}")
                print(f"    Size: {size_mb:.2f} MB")
                print(f"    Modified: {file_item.get('lastModifiedDateTime')}")
                if file_item.get('webUrl'):
                    print(f"    URL: {file_item.get('webUrl')}")
                print()

            return files
        except Exception as ex:
            print(f"Getting files failed: {ex}")
            raise ex

    def download_pmt_master_files(self, environment):
        """
        Download all PMT_MASTER files from the specified environment

        :param environment: 'dev' or 'prod' to specify which folder to use
        """
        # Validate environment
        if environment not in ['dev', 'prod']:
            raise ValueError("Environment must be 'dev' or 'prod'")

        # Get the root folder ID based on environment
        root_folder_id = DEV_ROOT_FOLDER_ID if environment == 'dev' else PROD_ROOT_FOLDER_ID
        print(f"Downloading PMT_MASTER files for {environment.upper()} environment...")

        # Create local directory
        local_dir = Path(environment)
        local_dir.mkdir(exist_ok=True)
        print(f"Created/verified local directory: {local_dir}")

        try:
            # Step 1: Get "All 835s" subfolder
            all_835s_folder = self._get_subfolder(root_folder_id, "All 835s")
            if not all_835s_folder:
                print("Could not find 'All 835s' folder")
                return []

            # Step 2: Get all payer_name subfolders
            payer_folders = self._get_folder_contents(all_835s_folder['id'])
            payer_folders = [item for item in payer_folders if 'folder' in item]
            print(f"Found {len(payer_folders)} payer folders")

            downloaded_files = []

            # Step 3: Process each payer folder
            for payer_folder in payer_folders:
                payer_name = payer_folder['name']
                print(f"Processing payer: {payer_name}")

                # Get files in payer folder
                payer_files = self._get_folder_contents(payer_folder['id'])

                # Print all files in this folder for debugging
                print(f"  All files in {payer_name} folder:")
                for file_item in payer_files:
                    file_type = 'FOLDER' if 'folder' in file_item else 'FILE'
                    print(f"    [{file_type}] {file_item.get('name')}")

                # Filter for PMT_MASTER files (updated pattern)
                pmt_master_files = [
                    f for f in payer_files
                    if 'file' in f and f['name'].endswith(f"_PMT MASTER_{payer_name}.xlsx")
                ]

                print(f"  Found {len(pmt_master_files)} PMT_MASTER files matching pattern '_PMT MASTER_{payer_name}.xlsx'")

                # Download each PMT_MASTER file
                for file_item in pmt_master_files:
                    file_path = self._download_file(file_item['id'], file_item['name'], local_dir)
                    if file_path:
                        downloaded_files.append(file_path)
                        print(f"  Downloaded: {file_item['name']}")

                print()  # Add blank line between payers

            print(f"Total files downloaded: {len(downloaded_files)}")
            return downloaded_files

        except Exception as ex:
            print(f"Error downloading PMT_MASTER files: {ex}")
            raise ex

    def _get_subfolder(self, parent_folder_id, subfolder_name):
        """Get a specific subfolder by name"""
        try:
            contents = self._get_folder_contents(parent_folder_id)
            for item in contents:
                if 'folder' in item and item['name'] == subfolder_name:
                    return item
            return None
        except Exception as ex:
            print(f"Error getting subfolder '{subfolder_name}': {ex}")
            return None

    def _get_folder_contents(self, folder_id):
        """Get contents of a folder by ID"""
        try:
            result = requests.get(
                f"{self.main_endpoint}/drives/{self.drive_id}/items/{folder_id}/children",
                headers={"Authorization": "Bearer " + self.access_token}
            )
            result.raise_for_status()
            return result.json().get('value', [])
        except Exception as ex:
            print(f"Error getting folder contents: {ex}")
            raise ex

    def _download_file(self, file_id, file_name, local_dir):
        """Download a file by ID to local directory"""
        try:
            # Get file content
            result = requests.get(
                f"{self.main_endpoint}/drives/{self.drive_id}/items/{file_id}/content",
                headers={"Authorization": "Bearer " + self.access_token}
            )
            result.raise_for_status()

            # Save file locally (overwrite if exists)
            file_path = local_dir / file_name
            with open(file_path, 'wb') as f:
                f.write(result.content)

            return str(file_path)
        except Exception as ex:
            print(f"Error downloading file '{file_name}': {ex}")
            return None


def main(environment):
    """Test the SharePoint connection and download PMT_MASTER files for specified environment"""
    try:
        client = SharePoint()

        # Show available files in root
        print("=== ROOT DOCUMENTS ===")
        files = client.get_files_in_documents()

        # Download PMT_MASTER files from specified environment
        print(f"\n=== DOWNLOADING PMT_MASTER FILES FOR {environment.upper()} ===")
        downloaded_files = client.download_pmt_master_files(environment)

        print(f"\nDownloaded {len(downloaded_files)} files to '{environment}' folder")

        return files
    except Exception as e:
        print(f"Error: {e}")
        return []


if __name__ == "__main__":
    main("prod")