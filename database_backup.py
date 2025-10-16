import os
import dropbox

# ============================================================
# 🔐 1️⃣ Environment Variables Required
# ------------------------------------------------------------
# Add these in your Render/GitHub secrets or .env file:
# DROPBOX_APP_KEY=your_app_key
# DROPBOX_APP_SECRET=your_app_secret
# DROPBOX_REFRESH_TOKEN=your_refresh_token
# ============================================================

DBX_PATH = "/estack.db"
LOCAL_DB = "estack.db"

def get_dbx():
    """Safely create Dropbox client using refresh token (auto-refresh forever)"""
    app_key = os.getenv("DROPBOX_APP_KEY")
    app_secret = os.getenv("DROPBOX_APP_SECRET")
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")

    if not all([app_key, app_secret, refresh_token]):
        raise ValueError("❌ Missing one or more Dropbox environment variables.")

    dbx = dropbox.Dropbox(
        app_key=app_key,
        app_secret=app_secret,
        oauth2_refresh_token=refresh_token
    )
    return dbx


def upload_db():
    """Upload local estack.db to Dropbox"""
    try:
        dbx = get_dbx()
        with open(LOCAL_DB, "rb") as f:
            dbx.files_upload(f.read(), DBX_PATH, mode=dropbox.files.WriteMode("overwrite"))
        print("✅ estack.db uploaded to Dropbox.")
    except FileNotFoundError:
        print("⚠️ Local estack.db not found for upload.")
    except Exception as e:
        print("❌ Dropbox upload failed:", e)


def download_db():
    """Download estack.db from Dropbox (run on app startup)"""
    try:
        dbx = get_dbx()
        metadata, res = dbx.files_download(DBX_PATH)
        with open(LOCAL_DB, "wb") as f:
            f.write(res.content)
        print("✅ estack.db downloaded from Dropbox.")
    except dropbox.exceptions.ApiError:
        print("⚠️ No existing estack.db found in Dropbox (starting fresh).")
    except Exception as e:
        print("❌ Dropbox download failed:", e)


# import os
# import dropbox

# DROPBOX_ACCESS_TOKEN = os.getenv("DROPBOX_TOKEN")
# DBX_PATH = "/estack.db"
# LOCAL_DB = "estack.db"

# def get_dbx():
#     if not DROPBOX_ACCESS_TOKEN:
#         raise ValueError("❌ Missing DROPBOX_TOKEN environment variable.")
#     return dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)

# def upload_db():
#     try:
#         dbx = get_dbx()
#         with open(LOCAL_DB, "rb") as f:
#             dbx.files_upload(f.read(), DBX_PATH, mode=dropbox.files.WriteMode("overwrite"))
#         print("✅ estack.db uploaded to Dropbox.")
#     except FileNotFoundError:
#         print("⚠️ Local estack.db not found for upload.")
#     except Exception as e:
#         print("❌ Dropbox upload failed:", e)

# def download_db():
#     try:
#         dbx = get_dbx()
#         metadata, res = dbx.files_download(DBX_PATH)
#         with open(LOCAL_DB, "wb") as f:
#             f.write(res.content)
#         print("✅ estack.db downloaded from Dropbox.")
#     except dropbox.exceptions.ApiError:
#         print("⚠️ No existing estack.db found in Dropbox (starting fresh).")
#     except Exception as e:
#         print("❌ Dropbox download failed:", e)
