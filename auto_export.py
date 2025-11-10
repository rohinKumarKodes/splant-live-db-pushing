#!/usr/bin/env python3
import sqlite3, csv, datetime, subprocess, os, sys, hashlib

# CONFIG
DB_FILE = "/home/s-plant-pi/splant-project/sensor_data.db"
EXPORT_DIR = "/home/s-plant-pi/splant-project/splant-live-db-pushing/exports"
REPO_DIR = "/home/s-plant-pi/splant-project/splant-live-db-pushing"
N = 50  # latest N readings to export
GIT_COMMIT_MSG = "Auto update: {fname}"

# Helpers
def ensure_dirs():
    os.makedirs(EXPORT_DIR, exist_ok=True)

def fetch_latest():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(f"SELECT id, timestamp, temperature, humidity, light, soil_moisture FROM readings ORDER BY id DESC LIMIT {N}")
    rows = c.fetchall()
    conn.close()
    # reverse to chronological order
    rows.reverse()
    return rows

def write_csv(rows):
    fname = f"data_snapshot_{datetime.date.today().isoformat()}.csv"
    path = os.path.join(EXPORT_DIR, fname)
    header = ["id", "timestamp", "temperature", "humidity", "light", "soil_moisture"]
    # Write to a temp file then atomically replace to avoid partial writes
    tmp = path + ".tmp"
    with open(tmp, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    os.replace(tmp, path)
    return path

def file_sha1(path):
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def git_commit_and_push(filepath):
    # run inside repo dir
    cwd = REPO_DIR
    relpath = os.path.relpath(filepath, cwd)
    # check if file already tracked and unchanged
    subprocess.run(["git", "add", relpath], cwd=cwd)
    # Only commit if there is a diff
    res = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=cwd)
    if res.returncode == 0:
        # nothing staged
        print("No changes to commit.")
        return
    msg = GIT_COMMIT_MSG.format(fname=os.path.basename(filepath))
    subprocess.run(["git", "commit", "-m", msg], cwd=cwd)
    push = subprocess.run(["git", "push"], cwd=cwd)
    if push.returncode != 0:
        print("git push failed", file=sys.stderr)

def main():
    ensure_dirs()
    rows = fetch_latest()
    if not rows:
        print("No rows returned; exiting.")
        return
    path = write_csv(rows)
    print("Wrote CSV:", path)
    git_commit_and_push(path)

if __name__ == "__main__":
    main()