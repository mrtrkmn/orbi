# This file uploads given file to Azure Blob Storage
# Usage: python azure-blob-storage.py <file/dir> <container> <blob>

import sys
import os
from azure.storage.blob import (
    BlobServiceClient,
)
from datetime import datetime
from datetime import timedelta

# import public_access
from azure.storage.blob import PublicAccess
from azure.storage.blob import AccessPolicy, ContainerSasPermissions


ACCOUNT_NAME = os.environ.get("ACCOUNT_NAME")
ACCOUNT_KEY = os.environ.get("ACCOUNT_KEY")
RETENTION_DAYS = os.environ.get("RETENTION_DAYS")


def upload_dir_content_to_azure(dir_path, blob_service_client: BlobServiceClient, container_name):
    container_client = blob_service_client.get_container_client(container=container_name)
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
    container_client = blob_service_client.get_container_client(container=container_name)
    with open(file=file_path, mode="rb") as data:
        blob_client = container_client.upload_blob(name=blob_name, data=data, overwrite=True)


def create_container(blob_service_client: BlobServiceClient, container_name):
    container_client = blob_service_client.get_container_client(container=container_name)
    # set container to public access
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
    container_client = blob_service_client.get_container_client(container=container_name)
    return container_client.url


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python azure-blob-storage.py <dir/file> <container> <blob>")
        sys.exit(1)
    blob_service_client = BlobServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net", credential=ACCOUNT_KEY
    )
    create_container(blob_service_client, sys.argv[2])
    if os.path.isdir(sys.argv[1]):
        upload_dir_content_to_azure(sys.argv[1], blob_service_client, sys.argv[2])
    else:
        upload_blob_file(sys.argv[1], blob_service_client, sys.argv[2], sys.argv[3])

    print(f"{get_public_access_url(blob_service_client, sys.argv[2])}/{sys.argv[1].split('/')[-1]}/")
