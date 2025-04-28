import os

import boto
import common


GALLERY_FOLDER = "/Users/ed/Desktop/Website Gallery/"
CLOUD_CONTAINER = "galleries"


def _user_creds():
    with open("docreds.rc") as ff:
        creds = ff.read()
    user_creds = {}
    for ln in creds.splitlines():
        if ln.startswith("spacekey"):
            user_creds["spacekey"] = ln.split("=")[-1].strip()
        elif ln.startswith("secret"):
            user_creds["secret"] = ln.split("=")[-1].strip()
        elif ln.startswith("bucket"):
            user_creds["bucket"] = ln.split("=")[-1].strip()
    return user_creds


def main():
    clt = common.create_S3_client()
    for root, dirs, files in os.walk(GALLERY_FOLDER, topdown=False):
        for name in files:
            if name.startswith("."):
                continue
            folder = root.split(GALLERY_FOLDER)[-1]
            local_name = os.path.join(folder, name)
            remote_path = os.path.join(CLOUD_CONTAINER, folder, name)
            if clt.get_key(remote_path):
                # Already exists
                print("Skipping", local_name)
                continue
            remote_file = clt.new_key(remote_path)
            local_path = os.path.join(GALLERY_FOLDER, folder, name)
            with open(local_path, "rb") as file_to_upload:
                remote_file.set_contents_from_file(file_to_upload)
            remote_file.set_acl("public-read")
            print("Uploaded", local_name)


if __name__ == "__main__":
    main()
