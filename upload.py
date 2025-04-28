import fcntl
from functools import partial
import hashlib
import logging
import mimetypes
import os
import sys
import tempfile
import warnings

from PIL import Image
import pyexif
import pymysql
import requests

import common
import utils


LOCKFILE = ".upload.lock"
PHOTODIR = "/Users/ed/Desktop/photoframe"
CLOUD_CONTAINER = "photoviewer"
DATABASE = "photoframe"
HASHFILE = "state.hash"
LOGFILE = "log/upload.log"
TESTING = False
THUMB_URL = "https://photo.leafe.com/images/thumb"
THUMB_SIZE = (120, 120)
LOG = None
DEFAULT_ENCONDING = "utf-8"

# Albums that have already been checked against the DB
seen_albums = {}


def _setup_logging():
    if not os.path.exists(LOGFILE):
        os.makedirs("log", exist_ok=True)
        with open(LOGFILE, "w") as ff:
            # This will create the file but not write anything
            pass
    global LOG
    LOG = logging.getLogger("upload")
    hnd = logging.FileHandler(LOGFILE)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    hnd.setFormatter(formatter)
    LOG.addHandler(hnd)
    if os.path.exists("LOGLEVEL"):
        with open("LOGLEVEL", "r") as ff:
            level = ff.read().strip()
    else:
        level = "INFO"
    logdebug("LEVEL:", level)
    LOG.setLevel(getattr(logging, level))


def logit(level, *msgs):
    if not LOG:
        _setup_logging()
    text = " ".join([f"{msg}" for msg in msgs])
    log_method = getattr(LOG, level)
    log_method(text)


logdebug = partial(logit, "debug")
loginfo = partial(logit, "info")


def directory_hash(dirname=""):
    dirname = PHOTODIR if not dirname else dirname
    cmd = f"ls -lhR {dirname}"
    out, err = common.runproc(cmd)
    m = hashlib.sha256(out.encode())
    ret = m.hexdigest()
    logdebug(f"Directory hash for {dirname}:", ret)
    return ret


def changed(subdir=None):
    if TESTING:
        return True
    match = "ALL" if subdir is None else os.path.basename(subdir)
    logdebug(f"Checking changed status of {match}")
    previous = None
    if os.path.exists(HASHFILE):
        with open(HASHFILE, "rb") as ff:
            ln = ff.readline()
            ln = ln.strip().decode(DEFAULT_ENCONDING)
            while ln:
                if isinstance(ln, bytes):
                    key, val = ln.decode(DEFAULT_ENCONDING).split(":")
                else:
                    key, val = ln.split(":")
                if key == match:
                    previous = val
                    break
                ln = ff.readline().strip()
        logdebug("Previous hash:", previous)
    if previous is None:
        # New directory
        return True
    dirname = PHOTODIR if subdir is None else os.path.join(PHOTODIR, subdir)
    curr = directory_hash(dirname)
    logdebug("Current hash:", curr)
    return curr != previous


def update_state():
    with open(HASHFILE, "w") as ff:
        dirhash = directory_hash(PHOTODIR)
        ff.write(f"ALL:{dirhash}\n")
        loginfo("State file updated")
        for fname in os.listdir(PHOTODIR):
            pth = os.path.join(PHOTODIR, fname)
            if fname.startswith(".") or not os.path.isdir(pth):
                continue
            dirhash = directory_hash(os.path.join(PHOTODIR, fname))
            #            if isinstance(fname, str):
            #                fname = fname.encode(DEFAULT_ENCONDING)
            ff.write(f"{fname}:{dirhash}\n")


def import_photos(clt, folder=None):
    if folder is None:
        folder = PHOTODIR
        album = None
    else:
        album = os.path.split(folder)[-1]
    # Update the database
    photos = [f for f in os.listdir(folder) if not f.startswith(".")]
    for photo_name in photos:
        fpath = os.path.join(folder, photo_name)
        if os.path.isdir(fpath):
            if changed(fpath):
                logdebug(f"Importing photos; directory '{fpath}' has changed")
                import_photos(clt, fpath)
            continue
        loginfo("Importing", photo_name)
        img = pyexif.ExifEditor(fpath)
        keywords = img.getKeywords()
        tags = img.getDictTags()
        file_type = tags.get("FileType", "")
        file_size = os.path.getsize(fpath)
        ht = tags.get("ImageHeight", 0)
        wd = tags.get("ImageWidth", 0)
        if ht == wd:
            orientation = "S"
        else:
            orientation = "H" if wd > ht else "V"
        # Use CreateDate if present; otherwise fall back to ModifiyDate
        created = img.getTag("CreateDate")
        if not created:
            created = img.getTag("ModifyDate")
        created = created or "1901:01:01 00:00:00"
        # The ExifEditor returns dates with all colons. Replace those that make
        # up the date portion.
        created = created.replace(":", "-", 2)
        # Update the DB record, if any
        add_or_update_db(
            photo_name, file_type, file_size, created, ht, wd, orientation, keywords, album
        )
        # If the image is smaller than 4000x3000, upscale it
        img_obj = Image.open(fpath)
        if orientation == "S":
            upscale = ht < 4000
            newsize = (4000, 4000)
        elif orientation == "H":
            upscale = wd < 4000
            newsize = (4000, 3000)
        else:
            upscale = ht < 4000
            newsize = (3000, 4000)
        if upscale:
            loginfo("Upscaling {} to {}".format(photo_name, newsize))
            img_obj.resize(newsize)
        with tempfile.TemporaryFile() as ff:
            loginfo("Uploading:", photo_name)
            img_obj.save(ff, format=file_type)
            remote_path = os.path.join(CLOUD_CONTAINER, photo_name)
            remote_file = clt.new_key(remote_path)
            content_type = mimetypes.guess_type(file_type)[0] or "image/jpg"
            ff.seek(0)
            remote_file.set_contents_from_file(ff, headers={"Content-Type": content_type})
            remote_file.set_acl("public-read")
        # Create a thumbnail to upload to the server
        img_obj = Image.open(fpath)
        img_obj.thumbnail(THUMB_SIZE)
        img_obj.filename = photo_name
        with tempfile.TemporaryFile() as ff:
            img_obj.save(ff, format=file_type)
            # Copy to the server
            ff.seek(0)
            files = {"thumb_file": ff.read()}
            data = {"filename": photo_name}
            loginfo("Posting thumbnail for", photo_name)
            resp = requests.post(THUMB_URL, data=data, files=files)

    # Finally, update the state
    update_state()


def add_or_update_db(
    photo_name, file_type, file_size, created, height, width, orientation, keywords, album
):
    crs = common.get_cursor(db=DATABASE)
    sql = "select * from image where name = %s;"
    crs.execute(sql, (photo_name,))
    recs = crs.fetchall()
    kw_str = " ".join(keywords)
    image_id = None
    if recs:
        loginfo("DB; image exists", photo_name)
        rec = recs[0]
        image_id = rec["pkid"]
        # Record exists; see if it differs
        if (
            (keywords == rec["keywords"])
            and (width == rec["wd"])
            and (ht == rec["height"])
            and (file_type == rec["imgtype"])
            and (orientation == rec["orientation"])
            and (created == rec["created"])
            and (file_size == rec["size"])
        ):
            # Everything matches; nothing to do.
            loginfo("DB; no change to", photo_name)
            pass
        else:
            sql = """update image set keywords = %s, width = %s, height = %s, imgtype = %s, orientation = %s,
            size = %s, created = %s where pkid = %s;"""
            crs.execute(
                sql, (kw_str, width, height, file_type, orientation, file_size, created, image_id)
            )
            loginfo("DB; updated", photo_name)
    else:
        # New image
        image_id = common.gen_uuid()
        sql = """insert into image (pkid, keywords, name, width, height, orientation, imgtype, size, created)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        crs.execute(
            sql,
            (
                image_id,
                kw_str,
                photo_name,
                width,
                height,
                orientation,
                file_type,
                file_size,
                created,
            ),
        )
        loginfo("DB; created record for", photo_name)
    if album:
        if isinstance(album, str):
            album = album.encode(DEFAULT_ENCONDING)
        album_id = seen_albums.get(album)
        if not album_id:
            sql = "select pkid from album where name = %s;"
            crs.execute(sql, (album,))
            rec = crs.fetchone()
            if rec:
                album_id = rec["pkid"]
                loginfo("DB; album", album, "exists")
            else:
                album_id = common.gen_uuid()
                sql = "insert into album (pkid, name) values (%s, %s);"
                crs.execute(sql, (album_id, album))
                loginfo("DB; created album", album)
            seen_albums[album] = album_id
        # Add the photo to the album`
        sql = """insert ignore into album_image set album_id = %s, image_id = %s;"""
        with warnings.catch_warnings():
            # Change filter action to 'error' to raise warnings as if they
            # were exceptions, to record them in the log file
            warnings.simplefilter("ignore", pymysql.Warning)
            crs.execute(sql, (album_id, image_id))
            loginfo("DB; Added", photo_name, "to album", album)
    crs.commit()


def processing():
    try:
        with open(LOCKFILE) as lockfile:
            fcntl.flock(lockfile, fcntl.LOCK_EX)
    except IOError:
        return True
    return False


if __name__ == "__main__":
    with open(LOCKFILE, "ab") as lockfile:
        try:
            fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            # Another process is running the upload
            loginfo("LOCKED!")
            exit()
        if changed():
            clt = utils.create_S3_client()
            import_photos(clt)
