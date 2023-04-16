# This file uploads given file to Azure Blob Storage
# Usage: python azure-blob-storage.py <file/dir> <container> <blob>

import sys
import os
from azure.storage.blob import (
    BlobServiceClient,
)
from datetime import datetime
from datetime import timedelta
from send_to_slack import send_message_to_slack

# import public_access
from azure.storage.blob import PublicAccess
from azure.storage.blob import AccessPolicy, ContainerSasPermissions

import argparse

root_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_path)

ACCOUNT_NAME = os.environ.get("ACCOUNT_NAME")
ACCOUNT_KEY = os.environ.get("ACCOUNT_KEY")
RETENTION_DAYS = os.environ.get("RETENTION_DAYS")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL")


def upload_dir_content_to_azure(dir_path, blob_service_client: BlobServiceClient, container_name):
    container_client = blob_service_client.get_container_client(container_name)
    # check suffix of file if it is .log then split it with '_' and take first part of it
    for file in os.listdir(dir_path):
        if file.endswith(".csv") or file.endswith(".xlsx"):
            # take last part of dir_path
            azure_blob_storage_dir_path = dir_path.split("/")[-1]

        with open(file=os.path.join(dir_path, file), mode="rb") as data:
            # create dir with timestamp
            blob_client = container_client.upload_blob(
                name="{}/{}".format(azure_blob_storage_dir_path, file), data=data, overwrite=True
            )


def upload_blob_file(file_path, blob_service_client: BlobServiceClient, container_name, blob_name):
    container_client = blob_service_client.get_container_client(container_name)
    with open(file=file_path, mode="rb") as data:
        blob_client = container_client.upload_blob(name=blob_name, data=data, overwrite=True)


def create_container(blob_service_client: BlobServiceClient, container_name):
    container_client = blob_service_client.get_container_client(container_name)
    # set container to public access
    RETENTION_DAYS = int(RETENTION_DAYS)
    try:
        container_client.create_container()
    except Exception as e:
        print(e)
    # set public access level to container
    access_policy = AccessPolicy(
        permission=ContainerSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(days=RETENTION_DAYS),
        start=datetime.utcnow() - timedelta(minutes=1),
    )

    identifiers = {"test": access_policy}
    container_client.set_container_access_policy(signed_identifiers=identifiers, public_access=PublicAccess.BLOB)


def get_public_access_url(blob_service_client: BlobServiceClient, container_name):
    container_client = blob_service_client.get_container_client(container_name)
    return container_client.url


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload file/dir to Azure Blob Storage.")
    parser.add_argument("--dir_file_path", type=str, help="The path to the file or data to upload.")
    parser.add_argument("--container_name", type=str, help="Container name to have on Azure Blob Storage.")
    parser.add_argument("--blob_name", type=str, help="Block name to have on Azure Blob Storage.")
    args = parser.parse_args()

    blob_service_client = BlobServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net", credential=ACCOUNT_KEY
    )

    create_container(blob_service_client, args.container_name)
    if os.path.isdir(args.dir_file_path):
        upload_dir_content_to_azure(args.dir_file_path, blob_service_client, args.container_name)
    else:
        upload_blob_file(args.file_path, blob_service_client, args.container_name, args.blob_name)

    message = f""""
    File uploaded to Azure Blob Storage: {get_public_access_url(blob_service_client, args.container_name)}/{args.dir_file_path.split('/')[-1]}/
    Uploaded files can be accessed for {RETENTION_DAYS} days.
    Files can be downloaded by appending file name to the above URL.
    e.g (for data generated on 26-03-2023)
        - {get_public_access_url(blob_service_client, args.container_name)}/{args.dir_file_path.split('/')[-1]}/orbis_data_licensee_26_03_2023.xlsx
        - {get_public_access_url(blob_service_client, args.container_name)}/{args.dir_file_path.split('/')[-1]}/orbis_data_licensee_26_03_2023.csv
    """

    send_message_to_slack(message, SLACK_CHANNEL)
