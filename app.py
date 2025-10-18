from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, g
import os, logging, sqlite3, json, requests, uuid
from datetime import datetime

import os
import dropbox
from flask_cors import CORS
from database_backup import download_db, upload_db  # ✅ Dropbox sync helpers

app = Flask(__name__)
CORS(app)

# ============================================================
#  🔹 Dropbox Auto Sync Section
# ============================================================

# Download the latest database on server startup
print("⏬ Checking Dropbox for latest estack.db...")
download_db()

def get_db():
    """Connect to SQLite database"""
    db = sqlite3.connect("estack.db", check_same_thread=False)
    db.row_factory = sqlite3.Row
    return db

# -------------------------
# API CONFIGURATION
# -------------------------
API_MODE = os.getenv("API_MODE", "sandbox")
SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")

DATABASE_sc = os.path.join(os.path.dirname(__file__), "transactions.db")

API_TOKEN = LIVE_API_TOKEN if API_MODE == "live" else SANDBOX_API_TOKEN
PAWAPAY_URL = (
    "https://api.pawapay.io/deposits"
    if API_MODE == "live"
    else "https://api.sandbox.pawapay.io/deposits"
)

PAWAPAY_PAYOUT_URL = (
    "https://api.pawapay.io/v2/payouts"
    if API_MODE == "live"
    else "https://api.sandbox.pawapay.io/v2/payouts"
)

# -------------------------
# DATABASE
# -------------------------
DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# JUST ADDED 1___________________________________________
def notify_investor(user_id, message):
    """
    Notify investor of investment status change.
    In real systems this could send email, SMS, or push.
    For now, we just log and store in a notifications table.
    """
    try:
        conn = sqlite3.connect(DATABASE)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                message TEXT,
                created_at TEXT
            )
        """)
        conn.commit()

        cur.execute("""
            INSERT INTO notifications (user_id, message, created_at)
            VALUES (?, ?, ?)
        """, (user_id, message, datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()

        logger.info(f"📢 Notification sent to investor {user_id}: {message}")
    except Exception as e:
        logger.error(f"❌ Failed to notify investor {user_id}: {e}")

def init_db_sc():
    """
    Create the transactions and loans tables if missing and safely add any missing columns.
    Also run a small backfill to populate 'type' and 'user_id' from metadata where possible.
    """
    conn = sqlite3.connect(DATABASE_sc)
    cur = conn.cursor()

    # Create wallets table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            balance REAL DEFAULT 0,
            currency TEXT DEFAULT 'ZMW',
            updated_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # =========================
    # ✅ TRANSACTIONS TABLE
    # =========================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            depositId TEXT UNIQUE,
            status TEXT,
            amount REAL,
            currency TEXT,
            phoneNumber TEXT,
            provider TEXT,
            providerTransactionId TEXT,
            failureCode TEXT,
            failureMessage TEXT,
            metadata TEXT,
            received_at TEXT,
            updated_at TEXT,
            created_at TEXT,
            type TEXT DEFAULT 'payment',
            user_id TEXT,
            investment_id TEXT,
            reference TEXT  -- ✅ added
        )
    """)
    conn.commit()

    cur.execute("PRAGMA table_info(transactions)")
    existing_cols = [r[1] for r in cur.fetchall()]

    needed = {
        "reference": "TEXT",
        "phoneNumber": "TEXT",
        "metadata": "TEXT",
        "updated_at": "TEXT",
        "created_at": "TEXT",
        "type": "TEXT DEFAULT 'payment'",
        "user_id": "TEXT",
        "investment_id": "TEXT"
    }

    for col, coltype in needed.items():
        if col not in existing_cols:
            try:
                cur.execute(f"ALTER TABLE transactions ADD COLUMN {col} {coltype}")
                logger.info("Added column %s to transactions table", col)
            except sqlite3.OperationalError:
                logger.warning("Could not add column %s (may already exist)", col)
    conn.commit()

        # =========================
    # ✅ LOANS TABLE (matches code)
    # =========================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS loans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            loanId TEXT UNIQUE,
            user_id TEXT,
            investment_id TEXT,
            amount REAL,
            interest REAL,
            status TEXT,
            expected_return_date TEXT,
            created_at TEXT,
            phone TEXT,
            metadata TEXT
        )
    """)
    conn.commit()

    cur.execute("PRAGMA table_info(loans)")
    existing_loan_cols = [r[1] for r in cur.fetchall()]

    loan_needed = {
        "loanId": "TEXT UNIQUE",
        "user_id": "TEXT",
        "investment_id": "TEXT",
        "amount": "REAL",
        "interest": "REAL",
        "status": "TEXT",
        "expected_return_date": "TEXT",
        "created_at": "TEXT",
        "phone": "TEXT",
        "metadata": "TEXT"
    }

    for col, coltype in loan_needed.items():
        if col not in existing_loan_cols:
            try:
                cur.execute(f"ALTER TABLE loans ADD COLUMN {col} {coltype}")
                logger.info("Added column %s to loans table", col)
            except sqlite3.OperationalError:
                logger.warning("Could not add column %s (may already exist)", col)

    conn.commit()

    # =========================
    # ✅ BACKFILL TRANSACTIONS
    # =========================
    try:
        cur.execute("SELECT depositId, metadata, type, user_id FROM transactions")
        rows = cur.fetchall()
        updates = []
        for deposit_id, metadata, cur_type, cur_user in rows:
            new_type = cur_type
            new_user = cur_user
            changed = False
            if metadata:
                try:
                    meta_obj = json.loads(metadata)
                except Exception:
                    meta_obj = None

                if isinstance(meta_obj, list):
                    for entry in meta_obj:
                        if not isinstance(entry, dict):
                            continue
                        fn = str(entry.get("fieldName") or "").lower()
                        fv = entry.get("fieldValue")
                        if fn == "userid" and fv and not new_user:
                            new_user = str(fv)
                            changed = True
                        if fn == "purpose" and isinstance(fv, str) and fv.lower() == "investment":
                            if new_type != "investment":
                                new_type = "investment"
                                changed = True
                elif isinstance(meta_obj, dict):
                    if "userId" in meta_obj and not new_user:
                        new_user = str(meta_obj.get("userId"))
                        changed = True
                    purpose = meta_obj.get("purpose")
                    if isinstance(purpose, str) and purpose.lower() == "investment":
                        if new_type != "investment":
                            new_type = "investment"
                            changed = True

            if new_type is None:
                new_type = "payment"

            if changed or (cur_user is None and new_user is not None) or (cur_type is None and new_type):
                updates.append((new_user, new_type, deposit_id))

        for u, t, dep in updates:
            cur.execute("UPDATE transactions SET user_id = ?, type = ? WHERE depositId = ?", (u, t, dep))
        if updates:
            conn.commit()
            logger.info("Backfilled %d transactions with user_id/type from metadata.", len(updates))
    except Exception:
        logger.exception("Error during migration/backfill pass")

    conn.close()


# ✅ Run safely within the Flask app context
with app.app_context():
    init_db_sc()

def get_db_sc():
    """
    Return a DB connection scoped to the Flask request context.
    Row factory is sqlite3.Row for dict-like rows.
    """
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE_sc)
        db.row_factory = sqlite3.Row
    return db

# -------------------------
# LOANS TABLE INIT
# -------------------------
def init_loans_table():
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS loans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        loanId TEXT UNIQUE,
        user_id TEXT,
        phone TEXT,                -- 🔹 NEW: borrower's phone number for payouts
        investment_id TEXT,        -- 🔹 links this loan to an investment
        amount REAL,
        interest REAL,
        status TEXT,               -- PENDING, APPROVED, DISAPPROVED, PAID
        expected_return_date TEXT,
        created_at TEXT,
        approved_by TEXT
    )
    """)

    conn.commit()
    conn.close()
    
with app.app_context():
    init_loans_table()

def migrate_loans_table():
    db = get_db_sc()
    existing_columns = [col["name"] for col in db.execute("PRAGMA table_info(loans)").fetchall()]

    # ✅ Ensure disbursed_at column exists
    if "disbursed_at" not in existing_columns:
        db.execute("ALTER TABLE loans ADD COLUMN disbursed_at TEXT")
        db.commit()
        print("✅ Added missing column: disbursed_at")

    db.close()


# ✅ run migrations safely once app starts
with app.app_context():
    init_db_sc()
    migrate_loans_table()


# -------------------------
# REQUEST A LOAN
# # -------------------------
import uuid
import sqlite3
from flask import Flask, jsonify, request
from datetime import datetime

app = Flask(__name__)

DB_PATH = "estack.db"

def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


# ------------------------
# 1️⃣ REQUEST A LOAN
# # ------------------------

# ------------------------
# 1️⃣ REQUEST A LOAN
# ------------------------
@app.route("/api/transactions/request", methods=["POST"])
def request_loan():
    try:
        data = request.get_json(force=True)
        phone = data.get("phone")
        amount = data.get("amount")
        investment_id = data.get("investment_id")

        print(f"📨 Received investment_id: {investment_id}")

        # ✅ Validate required fields
        if not phone or not investment_id or not amount:
            return jsonify({"error": "Missing required fields"}), 400

        db = get_db()
        cur = db.cursor()

        # ✅ Find investment that includes this ID and is COMPLETED
        cur.execute(
            """
            SELECT * FROM estack_transactions 
            WHERE name_of_transaction LIKE ? 
            AND status = 'COMPLETED'
            """,
            (f"%{investment_id}%",)
        )
        investment = cur.fetchone()

        if not investment:
            db.close()
            return jsonify({"error": "Investment not found or not completed"}), 404

        print(f"✅ Found matching investment: {investment['name_of_transaction']}")

        # ✅ Generate unique loan ID and name
        loan_id = str(uuid.uuid4())
        loan_name = f"LOAN | ZMW{amount} | {phone} | {investment_id} | {loan_id}"

        # ✅ Insert new loan record
        cur.execute(
            "INSERT INTO estack_transactions (name_of_transaction, status) VALUES (?, ?)",
            (loan_name, "ACTIVE")
        )

        # ✅ Mark investment as IN_USE
        cur.execute(
            "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
            ("IN_USE", f"%{investment_id}%")
        )

        db.commit()
        db.close()

        print(f"💰 Loan {loan_id} created for borrower {phone} using investment {investment_id}")

        return jsonify({
            "message": "Loan request recorded successfully",
            "loan_id": loan_id,
            "status": "ACTIVE"
        }), 200

    except Exception as e:
        print("❌ Error in /api/transactions/request:", e)
        return jsonify({"error": str(e)}), 500






# @app.route("/api/transactions/request", methods=["POST"])
# def request_loan():
#     try:
#         data = request.get_json(force=True)
#         phone = data.get("phone")
#         amount = data.get("amount")
#         # user_id = data.get("user_id")
#         investment_id = data.get("investment_id")

#         print(str(investment_id) + "from client")
#         # expected_return_date = data.get("expected_return_date", "")
#         # interest = data.get("interest", 0)

#         # if not phone or not user_id or not investment_id or not amount:
#         if not phone or not investment_id or not amount:
#             return jsonify({"error": "Missing required fields"}), 400

#         db = get_db()
#         cur = db.cursor()
# #_________________________________________________________________________________________________
#         # cur.execute("SELECT name_of_transaction FROM estack_transactions WHERE status = 'COMPLETED'")
#         cur.execute(
#                 "SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ? AND status = 'COMPLETED'",
#                 (f"%{investment_id}%",)
#             )
#         row = cur.fetchone()

#         if not row:
#             db.close()
#             return jsonify({"error": "No accepted transactions found"}), 404
        
#         name_of_transaction = row["name_of_transaction"]
        
#         # Extract investment ID (last element)
#         parts = [p.strip() for p in name_of_transaction.split("|")]
#         investment_id = parts[-1] if parts else None
        
#         if investment_id:
#             # ✅ Check if investment exists and is ACCEPTED
#             cur.execute(
#                 "SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ? AND status = 'COMPLETED'",
#                 (f"%{investment_id}%",)
#             )
#             # print(str(
# #_____________________________________________________________________________________________________________
#         # ✅ Check if investment exists and is ACCEPTED
#         # cur.execute("SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ?", (f"%{investment_id}%",))
#         investment = cur.fetchone()
#         # inv = cur.fetchone()
#         if investment:
#             print(f"✅ Found investment {investment_id}")
#         else:
#             print("❌ Investment not found or not accepted")
#         # else:
#         #     print("⚠️ Could not extract investment_id properly")
            
#         print(str(investment))
#         if not investment:
#             db.close()
#             print("Status1: " + str(investment["status"].upper()))# != "ACCEPTED"
#             return jsonify({"error": "Investment not found"}), 404

#         if investment["status"].upper() != "ACCEPTED":
#             db.close()
#             print("invest2: " + str(investment))
#             print("Status2: " + str(investment["status"].upper()))
#             return jsonify({"error": f"Investment not available (status={investment['status']})"}), 400

#         # ✅ Generate loan transaction
#         loan_id = str(uuid.uuid4())
#         loan_name = f"LOAN | ZMW{amount} | {investment_id} | {loan_id}"

#         # Insert loan transaction
#         cur.execute(
#             "INSERT INTO estack_transactions (name_of_transaction, status) VALUES (?, ?)",
#             (loan_name, "ACTIVE")
#         )

#         # Update linked investment to IN_USE
#         cur.execute(
#             "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
#             ("IN_USE", f"%{investment_id}%")
#         )

#         db.commit()
#         db.close()

#         print(f"💰 Loan {loan_id} created for user {user_id}, investment {investment_id}")

#         return jsonify({
#             "message": "Loan request recorded successfully",
#             "loanId": loan_id,
#             "status": "ACTIVE"
#         }), 200

#     except Exception as e:
#         print("❌ Error in /api/transactions/request:", e)
#         return jsonify({"error": str(e)}), 500


# # ------------------------
# # 2️⃣ GET USER LOANS
# # ------------------------
# @app.route("/api/loans/user/<user_id>", methods=["GET"])
# def get_user_loans(user_id):
#     try:
#         db = get_db()
#         cur = db.cursor()

#         cur.execute(
#             "SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ? AND name_of_transaction LIKE ?",
#             ("%LOAN%", f"%{user_id}%")
#         )
#         rows = cur.fetchall()
#         db.close()

#         results = []
#         for r in rows:
#             name = r["name_of_transaction"]
#             parts = [p.strip() for p in name.split("|")]
#             entry = {
#                 "name": name,
#                 "loanId": parts[-1] if len(parts) > 3 else "N/A",
#                 "amount": parts[1] if len(parts) > 1 else "N/A",
#                 "status": r["status"]
#             }
#             results.append(entry)

#         return jsonify(results), 200

#     except Exception as e:
#         print("❌ Error fetching loans:", e)
#         return jsonify({"error": str(e)}), 500

# ------------------------
# 2️⃣ GET USER LOANS
# ------------------------
@app.route("/api/loans/user/<user_id>", methods=["GET"])
def get_user_loans(user_id):
    try:
        db = get_db()
        cur = db.cursor()

        # Fetch transactions linked to this user (investor or borrower)
        cur.execute(
            """
            SELECT * FROM estack_transactions
            WHERE name_of_transaction LIKE ?
            ORDER BY rowid DESC
            """,
            (f"%{user_id}%",)
        )
        rows = cur.fetchall()
        db.close()

        results = []
        for r in rows:
            name = r["name_of_transaction"]
            parts = [p.strip() for p in name.split("|")]

            # Example:
            # INVESTMENT | K1000 | user_12 | 0f59ea4f-bc6d | Borrower:260977364437
            loan_id = parts[3] if len(parts) > 3 else "N/A"
            amount = parts[1].replace("K", "").strip() if len(parts) > 1 else "N/A"
            borrower = None

            # Check for borrower info
            if len(parts) > 4 and "Borrower:" in parts[4]:
                borrower = parts[4].split(":", 1)[1]

            entry = {
                "loan_id": loan_id,
                "amount": amount,
                "borrower": borrower or "N/A",
                "status": r["status"],
            }
            results.append(entry)

        return jsonify(results), 200

    except Exception as e:
        print("❌ Error fetching loans:", e)
        return jsonify({"error": str(e)}), 500

@app.route("/api/transactions/request", methods=["POST"])
def create_loan_request():
    try:
        data = request.get_json()
        print("data: " + str(data))
        required = ["borrower_phone", "investment_id", "amount"]
        missing = [f for f in required if f not in data]

        if missing:
            return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

        borrower_phone = data["borrower_phone"]
        investment_id = data["investment_id"]
        amount = data["amount"]

        conn = get_db()
        cur = conn.cursor()

        # 🔍 1️⃣ Check if investment exists and is available
        cur.execute(
            "SELECT name_of_transaction, status FROM estack_transactions WHERE name_of_transaction LIKE ?",
            (f"%{investment_id}%",)
        )
        investment = cur.fetchone()

        if not investment:
            conn.close()
            return jsonify({"error": "Investment not found"}), 404

        if investment["status"] != "AVAILABLE":
            conn.close()
            return jsonify({"error": "Investment already loaned or pending"}), 400

        # 🧩 2️⃣ Update record to include borrower details
        old_name = investment["name_of_transaction"]
        # Example: "INVESTMENT | K1000 | user_12 | 0f59ea4f-bc6d"
        new_name = f"{old_name} | Borrower:{borrower_phone}"

        cur.execute(
            "UPDATE estack_transactions SET name_of_transaction = ?, status = ? WHERE name_of_transaction = ?",
            (new_name, "REQUESTED", old_name)
        )

        conn.commit()
        conn.close()

        print(f"✅ Loan requested: {new_name}")

        return jsonify({
            "message": "Loan request recorded successfully",
            "investment_id": investment_id,
            "status": "REQUESTED"
        }), 200

    except Exception as e:
        print("❌ Error in /api/transactions/request:", e)
        return jsonify({"error": str(e)}), 500



# ------------------------
# 3️⃣ MARK LOAN AS REPAID
# ------------------------
@app.route("/api/loans/repay/<loan_id>", methods=["POST"])
def repay_loan(loan_id):
    try:
        db = get_db()
        cur = db.cursor()

        # Find the loan transaction
        cur.execute(
            "SELECT name_of_transaction FROM estack_transactions WHERE name_of_transaction LIKE ?",
            (f"%{loan_id}%",)
        )
        loan = cur.fetchone()

        if not loan:
            db.close()
            return jsonify({"error": "Loan not found"}), 404

        # Mark the loan as REPAID
        cur.execute(
            "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
            ("REPAID", f"%{loan_id}%")
        )

        # Extract user_id from transaction
        parts = [p.strip() for p in loan["name_of_transaction"].split("|")]
        user_id = parts[2] if len(parts) > 2 else None

        # Make the user's investment AVAILABLE again
        if user_id:
            cur.execute(
                """
                UPDATE estack_transactions
                SET status = ?
                WHERE name_of_transaction LIKE ?
                AND name_of_transaction NOT LIKE ?
                """,
                ("AVAILABLE", f"%{user_id}%", "%LOAN%")
            )

        db.commit()
        db.close()

        print(f"✅ Loan {loan_id} repaid — investment set to AVAILABLE")

        return jsonify({"message": "Loan repaid successfully"}), 200

    except Exception as e:
        print("❌ Error in repay_loan:", e)
        return jsonify({"error": str(e)}), 500

# # ------------------------
# # 3️⃣ MARK LOAN AS REPAID
# # ------------------------
# @app.route("/api/loans/repay/<loan_id>", methods=["POST"])
# def repay_loan(loan_id):
#     try:
#         db = get_db()
#         cur = db.cursor()

#         # ✅ Find the loan
#         cur.execute("SELECT name_of_transaction FROM estack_transactions WHERE name_of_transaction LIKE ?", (f"%{loan_id}%",))
#         loan = cur.fetchone()

#         if not loan:
#             db.close()
#             return jsonify({"error": "Loan not found"}), 404

#         # ✅ Mark loan as REPAID
#         cur.execute(
#             "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
#             ("REPAID", f"%{loan_id}%")
#         )

#         # ✅ Find linked investment ID (4th part of transaction)
#         parts = [p.strip() for p in loan["name_of_transaction"].split("|")]
#         user_id = parts[2] if len(parts) > 2 else None

#         # Mark investment AVAILABLE again
#         if user_id:
#             cur.execute(
#                 "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ? AND name_of_transaction NOT LIKE ?",
#                 ("AVAILABLE", f"%{user_id}%", "%LOAN%")
#             )

#         db.commit()
#         db.close()

#         print(f"✅ Loan {loan_id} repaid, investment reset to AVAILABLE")

#         return jsonify({"message": "Loan repaid successfully"}), 200

#     except Exception as e:
#         print("❌ Error in repay_loan:", e)
#         return jsonify({"error": str(e)}), 500

# @app.route("/api/loans/request", methods=["POST"])
# def request_loan():
#     data = request.json
#     user_id = data.get("user_id")
#     investment_id = data.get("investment_id")
#     amount = data.get("amount")
#     interest = data.get("interest", 5)
#     expected_return_date = data.get("expected_return_date")
#     phone = data.get("phone")  # <- NEW

#     if not user_id or not amount or not expected_return_date or not investment_id or not phone:
#         return jsonify({"error": "Missing required fields"}), 400

#     loanId = str(uuid.uuid4())
#     created_at = datetime.utcnow().isoformat()

#     conn = sqlite3.connect(DATABASE)
#     cur = conn.cursor()
#     cur.execute("""
#         INSERT INTO loans (loanId, user_id, investment_id, amount, interest, status, expected_return_date, created_at, phone)
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
#     """, (loanId, user_id, investment_id, amount, interest, "PENDING", expected_return_date, created_at, phone))
#     conn.commit()
#     conn.close()

#     return jsonify({"loanId": loanId, "status": "PENDING"}), 200

# -------------------------
# LIST PENDING LOANS (ADMIN VIEW)
# -------------------------
@app.route("/api/loans/pending", methods=["GET"])
def pending_loans():
    db = get_db()
    rows = db.execute("SELECT * FROM loans WHERE status='PENDING' ORDER BY created_at DESC").fetchall()
    results = [dict(row) for row in rows]
    return jsonify(results), 200


# -------------------------
# APPROVE LOAN
# # -------------------------
@app.route("/api/loans/approve/<loan_id>", methods=["POST"])
def approve_loan(loan_id):
    try:
        db = get_db()
        admin_id = request.json.get("admin_id", "admin_default")

        # ✅ Fetch loan by loanId
        loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
        if not loan:
            return jsonify({"error": "Loan not found"}), 404

        # ✅ Prevent double approval
        if loan["status"] and loan["status"].upper() == "APPROVED":
            return jsonify({"message": "Loan already approved"}), 200

        now = datetime.utcnow().isoformat()

        # ✅ Approve loan
        db.execute("""
            UPDATE loans
            SET status = 'APPROVED',
                approved_by = ?,
                approved_at = ?,
                updated_at = ?
            WHERE loanId = ?
        """, (admin_id, now, now, loan_id))

        # ✅ Update investor’s transaction using investment_id, not user_id
        if loan["investment_id"]:
            db.execute("""
                UPDATE transactions
                SET status = 'LOANED_OUT',
                    updated_at = ?,
                    failureMessage = 'Loan Approved',
                    failureCode = 'LOAN'
                WHERE depositId = ?
            """, (now, loan["investment_id"]))
            logger.info(f"✅ Investor transaction {loan['investment_id']} marked as LOANED_OUT.")

            # ✅ Notify investor
            txn = db.execute("SELECT user_id FROM transactions WHERE depositId=?", (loan["investment_id"],)).fetchone()
            if txn and txn["user_id"]:
                notify_investor(txn["user_id"], f"Your investment {loan['investment_id']} has been loaned out.")

        db.commit()
        return jsonify({"message": f"Loan {loan_id} approved and linked investor updated"}), 200

    except Exception as e:
        db.rollback()
        logger.exception("Error approving loan")
        return jsonify({"error": str(e)}), 500

# @app.route("/api/loans/approve/<loan_id>", methods=["POST"])
# def approve_loan(loan_id):
#     try:
#         db = get_db()
#         admin_id = request.json.get("admin_id", "admin_default")

#         # Check if loan exists
#         loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
#         if not loan:
#             return jsonify({"error": "Loan not found"}), 404

#         # Prevent double approval
#         if loan["status"].upper() == "APPROVED":
#             return jsonify({"message": "Loan already approved"}), 200

#         # Update loan status
#         db.execute("""
#             UPDATE loans
#             SET status = 'APPROVED',
#                 approved_by = ?,
#                 approved_at = ?,
#                 updated_at = ?
#             WHERE loanId = ?
#         """, (
#             admin_id,
#             datetime.utcnow().isoformat(),
#             datetime.utcnow().isoformat(),
#             loan_id
#         ))
#         db.commit()

#         # Update investor transaction if exists
#         db.execute("""
#             UPDATE transactions
#             SET status = 'LOANED_OUT',
#                 updated_at = ?,
#                 metadata = COALESCE(metadata, ''),
#                 failureMessage = 'Loan Approved',
#                 failureCode = 'LOAN'
#             WHERE user_id = ? AND type = 'investment'
#         """, (datetime.utcnow().isoformat(), loan["user_id"]))
#         db.commit()

#         return jsonify({"message": f"Loan {loan_id} approved successfully"}), 200

#     except Exception as e:
#         db.rollback()
#         return jsonify({"error": str(e)}), 500

# -------------------------
# DISAPPROVE LOAN
# -------------------------
@app.route("/api/loans/disapprove/<loan_id>", methods=["POST"])
def disapprove_loan(loan_id):
    db = get_db()
    db.execute("UPDATE loans SET status='DISAPPROVED' WHERE loanId=?", (loan_id,))
    db.commit()
    return jsonify({"message": "Loan disapproved"}), 200


# -------------------------
# INVESTOR LOANS VIEW
# -------------------------
@app.route("/api/loans/user/<user_id>", methods=["GET"])
def user_loans(user_id):
    db = get_db()
    rows = db.execute("SELECT * FROM loans WHERE user_id=? ORDER BY created_at DESC", (user_id,)).fetchall()
    results = [dict(row) for row in rows]
    return jsonify(results), 200

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


# -------------------------
# HEALTH
# -------------------------
@app.route("/")
def home():
    return f"PawaPay Callback Receiver running ✅ (API_MODE={API_MODE})"


# # -------------------------
# # ORIGINAL PAYMENT ENDPOINTS
# # -------------------------
# @app.route("/initiate-payment", methods=["POST"])
# def initiate_payment():
#     try:
#         data = request.json
#         phone = data.get("phone")
#         amount = data.get("amount")

#         if not phone or not amount:
#             return jsonify({"error": "Missing phone or amount"}), 400

#         deposit_id = str(uuid.uuid4())
#         customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

#         payload = {
#             "depositId": deposit_id,
#             "amount": str(amount),
#             "currency": "ZMW",
#             "correspondent": "MTN_MOMO_ZMB",
#             "payer": {"type": "MSISDN", "address": {"value": phone}},
#             "customerTimestamp": customer_ts,
#             "statementDescription": "StudyCraftPay",
#             "metadata": [
#                 {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
#                 {"fieldName": "customerId", "fieldValue": phone, "isPII": True},
#             ],
#         }

#         headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
#         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)
#         result = {}

#         try:
#             result = resp.json()
#         except Exception:
#             logger.warning("Non-JSON response from PawaPay for initiate-payment: %s", resp.text)

#         # 🧾 Removed all transaction.db writes
#         # ✅ Keep logs and return response for visibility
#         logger.info(
#             "Payment initiated: depositId=%s | phone=%s | amount=%s | status=%s",
#             deposit_id, phone, amount, result.get("status", "PENDING")
#         )

#         # ✅ Return result directly without saving to DB
#         return jsonify({
#             "depositId": deposit_id,
#             **result
#         }), 200

#     except Exception:
#         logger.exception("Payment initiation error")
#         return jsonify({"error": "Internal server error"}), 500

@app.route("/initiate-payment", methods=["POST"])
def initiate_payment():
    try:
        data = request.json
        phone = data.get("phone")
        amount = data.get("amount")
        correspondent - data.get("correspondent")
        if not phone or not amount:
            return jsonify({"error": "Missing phone or amount"}), 400

        deposit_id = str(uuid.uuid4())
        customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        payload = {
            "depositId": deposit_id,
            "amount": str(amount),
            "currency": "ZMW",
            "correspondent": str(correspondent), #"MTN_MOMO_ZMB",
            "payer": {"type": "MSISDN", "address": {"value": phone}},
            "customerTimestamp": customer_ts,
            "statementDescription": "StudyCraftPay",
            "metadata": [
                {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
                {"fieldName": "customerId", "fieldValue": phone, "isPII": True},
            ],
        }

        headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
        resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)
        result = {}
        try:
            result = resp.json()
        except Exception:
            logger.warning("Non-JSON response from PawaPay for initiate-payment: %s", resp.text)

        db = get_db_sc()
        db.execute("""
            INSERT OR REPLACE INTO transactions
            (depositId,status,amount,currency,phoneNumber,provider,
             providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            deposit_id,
            result.get("status", "PENDING"),
            float(amount),
            "ZMW",
            phone,
            None, None, None, None,
            json.dumps(payload["metadata"]),
            datetime.utcnow().isoformat(),
            "payment",
            None
        ))
        db.commit()
        logger.info("initiate-payment: inserted depositId=%s status=%s", deposit_id, result.get("status", "PENDING"))
        return jsonify({"depositId": deposit_id, **result}), 200

    except Exception:
        logger.exception("Payment initiation error")
        return jsonify({"error": "Internal server error"}), 500

# -------------------------
# CALLBACK RECEIVER (upsert-safe for deposits and payouts)
# -------------------------

#Test 2 callback 2
# @app.route("/callback/deposit", methods=["POST"])
# def deposit_callback():
#     try:
#         data = request.get_json(force=True)
#         print("📩 Full callback data:", data)

#         # Identify app type: StudyCraft vs eStack
#         metadata = data.get("metadata", {})
#         is_estack = isinstance(metadata, dict) and "userId" in metadata
#         is_studycraft = "payer" in data or "recipient" in data

#         # =====================================================
#         # 🔹 Case 1: eStack Application  ✅ (Unchanged)
#         # =====================================================
#         if is_estack:
#             deposit_id = data.get("depositId")
#             status = data.get("status", "PENDING").strip().upper()
#             amount = data.get("depositedAmount", 0)
#             user_id = metadata.get("userId", "unknown")

#             if not deposit_id:
#                 return jsonify({"error": "Missing depositId"}), 400

#             name_of_transaction = f"ZMW{amount} | {user_id} | {deposit_id}"

#             db = sqlite3.connect("estack.db")
#             db.row_factory = sqlite3.Row
#             cur = db.cursor()

#             cur.execute("""
#                 CREATE TABLE IF NOT EXISTS estack_transactions (
#                     name_of_transaction TEXT NOT NULL,
#                     status TEXT NOT NULL
#                 )
#             """)

#             existing = cur.execute(
#                 "SELECT name_of_transaction FROM estack_transactions WHERE name_of_transaction LIKE ?",
#                 (f"%{deposit_id}%",)
#             ).fetchone()

#             if existing:
#                 cur.execute(
#                     "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
#                     (status, f"%{deposit_id}%")
#                 )
#                 print(f"🔄 Updated eStack transaction {deposit_id} → {status}")
#             else:
#                 cur.execute(
#                     "INSERT INTO estack_transactions (name_of_transaction, status) VALUES (?, ?)",
#                     (name_of_transaction, status)
#                 )
#                 print(f"💾 Inserted new eStack transaction {deposit_id} → {status}")

#             db.commit()
#             db.close()

#             # ✅ Dropbox Sync (optional)
#             try:
#                 from database_backup import upload_db
#                 upload_db()
#             except Exception as sync_err:
#                 print("⚠️ Dropbox sync skipped:", sync_err)

#             return jsonify({"success": True, "source": "eStack", "deposit_id": deposit_id, "status": status}), 200

#         # =====================================================
#         # 🔹 Case 2: StudyCraft Application (NO DB WRITES)
#         # =====================================================
#         elif is_studycraft:
#             deposit_id = data.get("depositId")
#             payout_id = data.get("payoutId")

#             if not deposit_id and not payout_id:
#                 return jsonify({"error": "Missing depositId/payoutId"}), 400

#             txn_type = "payment" if deposit_id else "payout"
#             txn_id = deposit_id or payout_id
#             status = data.get("status")
#             amount = data.get("amount")
#             currency = data.get("currency")

#             if txn_type == "payment":
#                 phone = data.get("payer", {}).get("accountDetails", {}).get("phoneNumber")
#                 provider = data.get("payer", {}).get("accountDetails", {}).get("provider")
#             else:
#                 phone = data.get("recipient", {}).get("accountDetails", {}).get("phoneNumber")
#                 provider = data.get("recipient", {}).get("accountDetails", {}).get("provider")

#             provider_txn = data.get("providerTransactionId")
#             failure_code = data.get("failureReason", {}).get("failureCode")
#             failure_message = data.get("failureReason", {}).get("failureMessage")

#             user_id, loan_id = None, None
#             metadata_obj = metadata
#             if metadata_obj:
#                 if isinstance(metadata_obj, dict):
#                     user_id = metadata_obj.get("userId")
#                     loan_id = metadata_obj.get("loanId")
#                 elif isinstance(metadata_obj, list):
#                     for entry in metadata_obj:
#                         if isinstance(entry, dict):
#                             if entry.get("fieldName") == "userId":
#                                 user_id = entry.get("fieldValue")
#                             if entry.get("fieldName") == "loanId":
#                                 loan_id = entry.get("fieldValue")

#             # 🧾 Removed all transaction.db writes here.
#             # Still log info for debugging.
#             print("📘 StudyCraft callback received:")
#             print(f"   Type: {txn_type}")
#             print(f"   ID: {txn_id}")
#             print(f"   Status: {status}")
#             print(f"   Amount: {amount} {currency}")
#             print(f"   Phone: {phone} | Provider: {provider}")
#             print(f"   LoanID: {loan_id} | UserID: {user_id}")

#             # ✅ Return success response without touching transaction.db
#             return jsonify({
#                 "received": True,
#                 "source": "StudyCraft",
#                 "txn_type": txn_type,
#                 "txn_id": txn_id,
#                 "status": status
#             }), 200

#         # =====================================================
#         # 🔹 Unknown callback structure
#         # =====================================================
#         else:
#             return jsonify({"error": "Unknown callback format"}), 400

#     except Exception as e:
#         print("❌ Unified callback error:", e)
#         return jsonify({"error": str(e)}), 500

@app.route("/callback/deposit", methods=["POST"])
def deposit_callback():
    try:
        data = request.get_json(force=True)
        print("📩 Full callback data:", data)

        # Identify app type: StudyCraft vs eStack
        metadata = data.get("metadata", {})
        is_estack = isinstance(metadata, dict) and "userId" in metadata
        is_studycraft = "payer" in data or "recipient" in data

        # =====================================================
        # 🔹 Case 1: eStack Application
        # =====================================================
        if is_estack:
            deposit_id = data.get("depositId")
            status = data.get("status", "PENDING").strip().upper()
            amount = data.get("depositedAmount", 0)
            user_id = metadata.get("userId", "unknown")

            if not deposit_id:
                return jsonify({"error": "Missing depositId"}), 400

            name_of_transaction = f"ZMW{amount} | {user_id} | {deposit_id}"

            db = sqlite3.connect("estack.db")
            db.row_factory = sqlite3.Row
            cur = db.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS estack_transactions (
                    name_of_transaction TEXT NOT NULL,
                    status TEXT NOT NULL
                )
            """)

            existing = cur.execute(
                "SELECT name_of_transaction FROM estack_transactions WHERE name_of_transaction LIKE ?",
                (f"%{deposit_id}%",)
            ).fetchone()

            if existing:
                cur.execute(
                    "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
                    (status, f"%{deposit_id}%")
                )
                print(f"🔄 Updated eStack transaction {deposit_id} → {status}")
            else:
                cur.execute(
                    "INSERT INTO estack_transactions (name_of_transaction, status) VALUES (?, ?)",
                    (name_of_transaction, status)
                )
                print(f"💾 Inserted new eStack transaction {deposit_id} → {status}")

            db.commit()
            db.close()

            # ✅ Dropbox Sync (optional if using persistence)
            try:
                from database_backup import upload_db
                upload_db()
            except Exception as sync_err:
                print("⚠️ Dropbox sync skipped:", sync_err)

            return jsonify({"success": True, "source": "eStack", "deposit_id": deposit_id, "status": status}), 200

        # =====================================================
        # 🔹 Case 2: StudyCraft Application
        # =====================================================
        elif is_studycraft:
            deposit_id = data.get("depositId")
            payout_id = data.get("payoutId")

            if not deposit_id and not payout_id:
                return jsonify({"error": "Missing depositId/payoutId"}), 400

            txn_type = "payment" if deposit_id else "payout"
            txn_id = deposit_id or payout_id
            status = data.get("status")
            amount = data.get("amount")
            currency = data.get("currency")

            if txn_type == "payment":
                phone = data.get("payer", {}).get("accountDetails", {}).get("phoneNumber")
                provider = data.get("payer", {}).get("accountDetails", {}).get("provider")
            else:
                phone = data.get("recipient", {}).get("accountDetails", {}).get("phoneNumber")
                provider = data.get("recipient", {}).get("accountDetails", {}).get("provider")

            provider_txn = data.get("providerTransactionId")
            failure_code = data.get("failureReason", {}).get("failureCode")
            failure_message = data.get("failureReason", {}).get("failureMessage")

            user_id, loan_id = None, None
            metadata_obj = metadata
            if metadata_obj:
                if isinstance(metadata_obj, dict):
                    user_id = metadata_obj.get("userId")
                    loan_id = metadata_obj.get("loanId")
                elif isinstance(metadata_obj, list):
                    for entry in metadata_obj:
                        if isinstance(entry, dict):
                            if entry.get("fieldName") == "userId":
                                user_id = entry.get("fieldValue")
                            if entry.get("fieldName") == "loanId":
                                loan_id = entry.get("fieldValue")

            db = get_db_sc()
            existing = db.execute(
                "SELECT * FROM transactions WHERE depositId=? OR depositId=?",
                (deposit_id, payout_id)
            ).fetchone()

            now_iso = datetime.utcnow().isoformat()
            metadata_str = json.dumps(metadata_obj) if metadata_obj else None

            if existing:
                db.execute("""
                    UPDATE transactions
                    SET status = COALESCE(?, status),
                        amount = COALESCE(?, amount),
                        currency = COALESCE(?, currency),
                        phoneNumber = COALESCE(?, phoneNumber),
                        provider = COALESCE(?, provider),
                        providerTransactionId = COALESCE(?, providerTransactionId),
                        failureCode = COALESCE(?, failureCode),
                        failureMessage = COALESCE(?, failureMessage),
                        metadata = COALESCE(?, metadata),
                        updated_at = ?,
                        user_id = COALESCE(?, user_id)
                    WHERE depositId = ? OR depositId = ?
                """, (
                    status,
                    float(amount) if amount else None,
                    currency,
                    phone,
                    provider,
                    provider_txn,
                    failure_code,
                    failure_message,
                    metadata_str,
                    now_iso,
                    user_id,
                    deposit_id,
                    payout_id
                ))
            else:
                db.execute("""
                    INSERT INTO transactions
                    (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId,
                     failureCode, failureMessage, metadata, received_at, updated_at, type, user_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    txn_id,
                    status,
                    float(amount) if amount else None,
                    currency,
                    phone,
                    provider,
                    provider_txn,
                    failure_code,
                    failure_message,
                    metadata_str,
                    now_iso,
                    now_iso,
                    txn_type,
                    user_id
                ))

            # ✅ Handle loan repayment notification
            if txn_type == "payout" and loan_id and status in ("COMPLETED", "SUCCESS", "PAYMENT_COMPLETED"):
                db.execute("UPDATE loans SET status=? WHERE loanId=?", (status, loan_id))
                loan_row = db.execute("SELECT user_id FROM loans WHERE loanId=?", (loan_id,)).fetchone()
                if loan_row and loan_row["user_id"]:
                    notify_investor(
                        loan_row["user_id"],
                        f"Loan {loan_id[:8]} has been successfully repaid."
                    )

            db.commit()
            return jsonify({"received": True, "source": "StudyCraft"}), 200

        # =====================================================
        # 🔹 Unknown callback structure
        # =====================================================
        else:
            return jsonify({"error": "Unknown callback format"}), 400

    except Exception as e:
        print("❌ Unified callback error:", e)
        return jsonify({"error": str(e)}), 500

# -------------------------
# DEPOSIT STATUS / TRANSACTION LOOKUP
# -------------------------
# @app.route("/deposit_status/<deposit_id>")
# def deposit_status(deposit_id):
#     db = get_db()
#     row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
#     if not row:
#         return jsonify({"status": None, "message": "Deposit not found"}), 404
#     res = {k: row[k] for k in row.keys()}
#     if res.get("metadata"):
#         try:
#             res["metadata"] = json.loads(res["metadata"])
#         except:
#             pass
#     return jsonify(res), 200

# @app.route("/transactions/<deposit_id>")
# def get_transaction(deposit_id):
#     db = get_db()
#     row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
#     if not row:
#         return jsonify({"error": "not found"}), 404
#     res = {k: row[k] for k in row.keys()}
#     if res.get("metadata"):
#         try:
#             res["metadata"] = json.loads(res["metadata"])
#         except:
#             pass
#     return jsonify(res), 200

# -------------------------
# INVESTMENT ENDPOINTS
# -------------------------
# @app.route("/api/investments/initiate", methods=["POST"])
# def initiate_investment():
#     try:
#         data = request.json or {}
#         # Support both "phone" and "phoneNumber" keys from different clients
#         phone = data.get("phone") or data.get("phoneNumber")
#         amount = data.get("amount")
#         correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
#         currency = data.get("currency", "ZMW")
#         # prefer explicit user_id, but don't crash if missing
#         user_id = data.get("user_id") or data.get("userId") or "unknown"

#         if not phone or amount is None:
#             return jsonify({"error": "Missing phone or amount"}), 400

#         deposit_id = str(uuid.uuid4())
#         customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

#         payload = {
#             "depositId": deposit_id,
#             "amount": str(amount),
#             "currency": currency,
#             "correspondent": correspondent,
#             "payer": {"type": "MSISDN", "address": {"value": phone}},
#             "customerTimestamp": customer_ts,
#             "statementDescription": "Investment",
#             "metadata": [
#                 {"fieldName": "purpose", "fieldValue": "investment"},
#                 {"fieldName": "userId", "fieldValue": str(user_id), "isPII": True},
#             ],
#         }

#         headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
#         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)

#         try:
#             result = resp.json()
#         except Exception:
#             logger.error("PawaPay response not JSON: %s", resp.text)
#             return jsonify({"error": "Invalid response from PawaPay"}), 502

#         status = result.get("status", "PENDING")

#         db = get_db()
#         # Insert a new investment record (depositId will be unique)
#         db.execute("""
#             INSERT OR REPLACE INTO transactions
#             (depositId,status,amount,currency,phoneNumber,provider,
#              providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
#             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
#         """, (
#             deposit_id,
#             status,
#             float(amount),
#             currency,
#             phone,
#             None, None, None, None,
#             json.dumps(payload["metadata"]),
#             datetime.utcnow().isoformat(),
#             "investment",
#             user_id
#         ))
#         db.commit()
#         logger.info("initiate_investment: inserted depositId=%s user_id=%s amount=%s status=%s",
#                     deposit_id, user_id, amount, status)

#         return jsonify({"depositId": deposit_id, "status": status}), 200

#     except Exception as e:
#         logger.exception("Investment initiation error")
#         return jsonify({"error": str(e)}), 500


# @app.route("/api/investments/user/<user_id>", methods=["GET"])
# def get_user_investments(user_id):
#     """
#     Return investments for a user. We select type='investment' and the exact user_id column.
#     This returns a list of rows (may be empty).
#     """
#     db = get_db()
#     rows = db.execute(
#         "SELECT * FROM transactions WHERE type='investment' AND user_id=? ORDER BY received_at DESC",
#         (user_id,)
#     ).fetchall()

#     results = []
#     for row in rows:
#         res = {k: row[k] for k in row.keys()}
#         if res.get("metadata"):
#             try:
#                 res["metadata"] = json.loads(res["metadata"])
#             except:
#                 pass
#         results.append(res)

#     return jsonify(results), 200


# # -------------------------
# # SAMPLE INVESTMENT ROUTE (handy for testing)
# # -------------------------
# @app.route("/sample-investment", methods=["POST"])
# def add_sample():
#     """Add a test investment to verify DB works"""
#     try:
#         db = get_db()
#         deposit_id = str(uuid.uuid4())
#         payload_metadata = [{"fieldName": "purpose", "fieldValue": "investment"},
#                             {"fieldName": "userId", "fieldValue": "user_1"}]
#         received_at = datetime.utcnow().isoformat()
#         db.execute("""
#             INSERT INTO transactions
#             (depositId,status,amount,currency,phoneNumber,metadata,received_at,type,user_id)
#             VALUES (?,?,?,?,?,?,?,?,?)
#         """, (
#             deposit_id,
#             "SUCCESS",
#             1000.0,
#             "ZMW",
#             "0965123456",
#             json.dumps(payload_metadata),
#             received_at,
#             "investment",
#             "user_1"
#         ))
#         db.commit()
#         logger.info("Added sample investment depositId=%s", deposit_id)
#         return jsonify({"message":"Sample investment added","depositId":deposit_id}), 200
#     except Exception as e:
#         logger.exception("Failed to insert sample")
#         return jsonify({"error": str(e)}), 500


# -------------------------
# OPTIONAL: debug route to see all transactions (helpful during testing)
# -------------------------
@app.route("/debug/transactions", methods=["GET"])
def debug_transactions():
    db = get_db_sc()
    rows = db.execute("SELECT * FROM transactions ORDER BY received_at DESC").fetchall()
    results = []
    for row in rows:
        res = {k: row[k] for k in row.keys()}
        if res.get("metadata"):
            try:
                res["metadata"] = json.loads(res["metadata"])
            except:
                pass
        results.append(res)
    return jsonify(results), 200
    
#-----------------------------------
# GET PENDING REQUESTS
#----------------------------------

# @app.route("/api/loans/pending", methods=["GET"])
# def get_pending_loans():
#     conn = sqlite3.connect(DATABASE)
#     cur = conn.cursor()
#     cur.execute("SELECT loanId, user_id, amount, interest, status, expected_return_date FROM loans WHERE status = ?", ("PENDING",))
#     rows = cur.fetchall()
#     conn.close()

#     loans = []
#     for row in rows:
#         loans.append({
#             "loanId": row[0],
#             "user_id": row[1],
#             "amount": row[2],
#             "interest": row[3],
#             "status": row[4],
#             "expected_return_date": row[5]
#         })

#     return jsonify(loans), 200

# -------------------------
# DISBURSE LOAN (ADMIN ACTION)
# -------------------------

#TEST 4
@app.route("/api/loans/disburse/<loan_id>", methods=["POST"])
def disburse_loan(loan_id):
    try:
        db = get_db_sc()  # ✅ Get the database connection
        data = request.get_json() or {}
        logger.info(f"Disbursing loan {loan_id} with data: {data}")

        # ✅ Fetch loan details (fixed column name)
        loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
        if not loan:
            return jsonify({"error": "Loan not found"}), 404

        # ✅ Ensure loan is approved before disbursement
        # if loan["status"] != "approved":
        #     return jsonify({"error": "Loan is not approved for disbursement"}), 400

        borrower_id = loan["user_id"]
        amount = float(loan["amount"])

        # ✅ Fetch borrower wallet
        borrower_wallet = db.execute(
            "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
        ).fetchone()
        
        if not borrower_wallet:
            db.execute("""
                INSERT INTO wallets (user_id, balance, created_at, updated_at)
                VALUES (?, 0, ?, ?)
            """, (borrower_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat()))
            db.commit()
            logger.info(f"✅ Created new wallet for borrower {borrower_id}")
        
            borrower_wallet = db.execute(
                "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
            ).fetchone()

        borrower_balance = float(borrower_wallet["balance"])

        # ✅ Credit borrower wallet
        new_balance = borrower_balance + amount
        db.execute(
            "UPDATE wallets SET balance = ?, updated_at = ? WHERE user_id = ?",
            (new_balance, datetime.utcnow().isoformat(), borrower_id)
        )

        # ✅ Mark loan as disbursed (fixed column name)
        db.execute(
            "UPDATE loans SET status = 'disbursed', disbursed_at = ? WHERE loanId = ?",
            (datetime.utcnow().isoformat(), loan_id)
        )

        # ✅ Record the disbursement transaction
        db.execute("""
            INSERT INTO transactions (user_id, amount, type, status, reference, created_at, updated_at)
            VALUES (?, ?, 'loan_disbursement', 'SUCCESS', ?, ?, ?)
        """, (
            borrower_id, amount, loan_id,
            datetime.utcnow().isoformat(),
            datetime.utcnow().isoformat()
        ))

        db.commit()

        # ✅ Link this loan to one available investment
        try:
            # investment_row = db.execute("""
            #     SELECT depositId FROM transactions
            #     WHERE type = 'investment' AND status = 'ACTIVE'
            #     ORDER BY received_at ASC LIMIT 1
            # """).fetchone()
            investment_row = db.execute("""
                SELECT reference FROM transactions
                WHERE type = 'investment' AND status = 'ACTIVE'
                ORDER BY created_at ASC LIMIT 1
            """).fetchone()


            if investment_row:
                # investment_id = investment_row["depositId"]
                investment_id = investment_row["reference"]


                # ✅ Mark that single investment as LOANED_OUT
                db.execute("""
                    UPDATE transactions
                    SET status = 'LOANED_OUT', updated_at = ?
                    WHERE reference = ?

                    # UPDATE transactions
                    # SET status = 'LOANED_OUT', investment_id = ?, updated_at = ?
                    # WHERE depositId = ?
                """, (loan_id, datetime.utcnow().isoformat(), investment_id))
                db.commit()

                # ✅ Notify the investor
                investor_row = db.execute("""
                    SELECT user_id FROM transactions
                    WHERE reference = ? AND type = 'investment'
                    # SELECT user_id FROM transactions
                    # WHERE depositId = ? AND type = 'investment'
                """, (investment_id,)).fetchone()
                # investment_row = db.execute("""
                #     SELECT reference FROM transactions
                #     WHERE type = 'investment' AND status = 'ACTIVE'
                #     ORDER BY created_at ASC LIMIT 1
                # """).fetchone()

                # ✅ Also mark investor's transaction as DISBURSED
                if loan["investment_id"]:
                    db.execute("""
                        UPDATE transactions
                        SET status = 'DISBURSED', updated_at = ?
                        WHERE depositId = ?
                    """, (datetime.utcnow().isoformat(), loan["investment_id"]))
                    logger.info(f"✅ Investor transaction {loan['investment_id']} marked as DISBURSED.")



                if investor_row and investor_row["user_id"]:
                    notify_investor(
                        investor_row["user_id"],
                        f"Your investment {investment_id[:8]} has been loaned out to borrower {loan_id[:8]}."
                    )

                logger.info(f"Investment {investment_id} linked to loan {loan_id}")
            else:
                logger.warning("No available active investment found to link with this loan.")

        except Exception as e:
            logger.error(f"Error linking investment to loan {loan_id}: {e}")

        return jsonify({
            "message": f"Loan {loan_id} successfully disbursed",
            "borrower_id": borrower_id,
            "amount": amount,
            "new_balance": new_balance
        }), 200

    except Exception as e:
        logger.error(f"Error disbursing loan {loan_id}: {e}")
        return jsonify({"error": str(e)}), 500

# -------------------------
# REJECT LOAN (ADMIN ACTION)
# -------------------------
@app.route("/api/loans/reject/<loan_id>", methods=["POST"])
def reject_loan(loan_id):
    admin_id = request.json.get("admin_id", "admin_default")
    db = get_db_sc()
    loan = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
    if not loan:
        return jsonify({"error": "Loan not found"}), 404
    if loan["status"] != "PENDING":
        return jsonify({"error": f"Loan already {loan['status']}"}), 400

    db.execute("UPDATE loans SET status='REJECTED', approved_by=? WHERE loanId=?", (admin_id, loan_id))
    db.commit()

    return jsonify({"loanId": loan_id, "status": "REJECTED"}), 200

# OPTIONAL CODE CHECK NOTIFICATION 
@app.route("/api/notifications/<user_id>", methods=["GET"])
def get_notifications(user_id):
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC", (user_id,))
    rows = cur.fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows]), 200

# -------------------------
# RUN
# -------------------------

if __name__ == "__main__":
    with app.app_context():
        init_db()
        init_db_sc()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)


# from dotenv import load_dotenv
# load_dotenv()

# from flask import Flask, request, jsonify, g
# import os, logging, sqlite3, json, requests, uuid
# from datetime import datetime

# # -------------------------
# # API CONFIGURATION
# # -------------------------
# API_MODE = os.getenv("API_MODE", "sandbox")
# SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
# LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")

# API_TOKEN = LIVE_API_TOKEN if API_MODE == "live" else SANDBOX_API_TOKEN
# PAWAPAY_URL = (
#     "https://api.pawapay.io/deposits"
#     if API_MODE == "live"
#     else "https://api.sandbox.pawapay.io/deposits"
# )

# PAWAPAY_PAYOUT_URL = (
#     "https://api.pawapay.io/v2/payouts"
#     if API_MODE == "live"
#     else "https://api.sandbox.pawapay.io/v2/payouts"
# )

# # -------------------------
# # DATABASE
# # -------------------------
# DATABASE_2 = os.path.join(os.path.dirname(__file__), "estack.db")
# app = Flask(__name__)
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# =========================
# ✅ DATABASE CONFIG
# =========================
DATABASE = os.path.join(os.path.dirname(__file__), "estack.db")

def init_db():
    """
    Create the estack_transactions table if missing.
    Stores combined transaction info and status only.
    """
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    # ✅ Create the single table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS estack_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name_of_transaction TEXT NOT NULL,  -- e.g. "K1000 | user_123 | DEP4567"
            status TEXT NOT NULL,               -- e.g. "invested", "loaned_out", "repaid"
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    print("✅ estack.db initialized with estack_transactions table.")


# ✅ Initialize database once Flask app starts
with app.app_context():
    init_db()


def get_db():
    """
    Return a DB connection scoped to the Flask request context.
    Row factory is sqlite3.Row for dict-like access.
    """
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db
    
# # -------------------------
# # REQUEST A LOAN
# @app.route("/api/transactions/request", methods=["POST"])
# def request_loan():
#     try:
#         data = request.json or {}
#         borrower_id = data.get("borrower_id")
#         phone = data.get("phone")
#         amount = data.get("amount")
#         investment_id = data.get("investment_id")
#         interest = data.get("interest", 5)
#         expected_return_date = data.get("expected_return_date", "")

#         if not borrower_id or not amount:
#             return jsonify({"error": "Missing borrower_id or amount"}), 400

#         # Combine into readable name for same table format
#         # Example: "ZMW500 | user_1 | loan_abc123"
#         loan_id = str(uuid.uuid4())
#         name = f"ZMW{amount} | {borrower_id} | {loan_id}"
#         status = "REQUESTED"

#         db = get_db_2()
#         db.execute(
#             "INSERT INTO transactions (name, status) VALUES (?, ?)",
#             (name, status)
#         )
#         db.commit()

#         print(f"💸 Loan request recorded for {borrower_id}: {amount} ({loan_id})")

#         return jsonify({
#             "message": "Loan requested successfully",
#             "loan_id": loan_id,
#             "amount": amount,
#             "status": status
#         }), 200

#     except Exception as e:
#         logger.exception("Error requesting loan")
#         return jsonify({"error": str(e)}), 500

# # -------------------------
# # LIST PENDING LOANS (ADMIN VIEW)
# # -------------------------
# @app.route("/api/loans/pending", methods=["GET"])
# def pending_loans():
#     db = get_db_2()
#     rows = db.execute("SELECT * FROM loans WHERE status='PENDING' ORDER BY created_at DESC").fetchall()
#     results = [dict(row) for row in rows]
#     return jsonify(results), 200

# # -------------------------
# # APPROVE LOAN
# # # -------------------------
# @app.route("/api/loans/approve/<loan_id>", methods=["POST"])
# def approve_loan(loan_id):
#     try:
#         db = get_db_2()
#         admin_id = request.json.get("admin_id", "admin_default")

#         # ✅ Fetch loan by loanId
#         loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
#         if not loan:
#             return jsonify({"error": "Loan not found"}), 404

#         # ✅ Prevent double approval
#         if loan["status"] and loan["status"].upper() == "APPROVED":
#             return jsonify({"message": "Loan already approved"}), 200

#         now = datetime.utcnow().isoformat()

#         # ✅ Approve loan
#         db.execute("""
#             UPDATE loans
#             SET status = 'APPROVED',
#                 approved_by = ?,
#                 approved_at = ?,
#                 updated_at = ?
#             WHERE loanId = ?
#         """, (admin_id, now, now, loan_id))

#         # ✅ Update investor’s transaction using investment_id, not user_id
#         if loan["investment_id"]:
#             db.execute("""
#                 UPDATE transactions
#                 SET status = 'LOANED_OUT',
#                     updated_at = ?,
#                     failureMessage = 'Loan Approved',
#                     failureCode = 'LOAN'
#                 WHERE depositId = ?
#             """, (now, loan["investment_id"]))
#             logger.info(f"✅ Investor transaction {loan['investment_id']} marked as LOANED_OUT.")

#             # ✅ Notify investor
#             txn = db.execute("SELECT user_id FROM transactions WHERE depositId=?", (loan["investment_id"],)).fetchone()
#             if txn and txn["user_id"]:
#                 notify_investor(txn["user_id"], f"Your investment {loan['investment_id']} has been loaned out.")

#         db.commit()
#         return jsonify({"message": f"Loan {loan_id} approved and linked investor updated"}), 200

#     except Exception as e:
#         db.rollback()
#         logger.exception("Error approving loan")
#         return jsonify({"error": str(e)}), 500


# # -------------------------
# # DISAPPROVE LOAN
# # -------------------------
# @app.route("/api/loans/disapprove/<loan_id>", methods=["POST"])
# def disapprove_loan(loan_id):
#     db = get_db_2()
#     db.execute("UPDATE loans SET status='DISAPPROVED' WHERE loanId=?", (loan_id,))
#     db.commit()
#     return jsonify({"message": "Loan disapproved"}), 200

# # -------------------------
# # INVESTOR LOANS VIEW
# # -------------------------
# @app.route("/api/loans/user/<user_id>", methods=["GET"])
# def user_loans(user_id):
#     db = get_db_2()
#     rows = db.execute("SELECT * FROM loans WHERE user_id=? ORDER BY created_at DESC", (user_id,)).fetchall()
#     results = [dict(row) for row in rows]
#     return jsonify(results), 200

# @app.teardown_appcontext
# def close_connection(exception):
#     db = getattr(g, "_database", None)
#     if db is not None:
#         db.close()

# # -------------------------
# # HEALTH
# # -------------------------
# @app.route("/")
# def home():
#     return f"PawaPay Callback Receiver running ✅ (API_MODE={API_MODE})"

# # -------------------------
# # ORIGINAL PAYMENT ENDPOINTS
# # -------------------------
# @app.route("/initiate-payment", methods=["POST"])
# def initiate_payment():
#     try:
#         data = request.json
#         phone = data.get("phone")
#         amount = data.get("amount")
#         if not phone or not amount:
#             return jsonify({"error": "Missing phone or amount"}), 400

#         deposit_id = str(uuid.uuid4())
#         customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

#         payload = {
#             "depositId": deposit_id,
#             "amount": str(amount),
#             "currency": "ZMW",
#             "correspondent": "MTN_MOMO_ZMB",
#             "payer": {"type": "MSISDN", "address": {"value": phone}},
#             "customerTimestamp": customer_ts,
#             "statementDescription": "StudyCraftPay",
#             "metadata": [
#                 {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
#                 {"fieldName": "customerId", "fieldValue": phone, "isPII": True},
#             ],
#         }

#         headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
#         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)
#         result = {}
#         try:
#             result = resp.json()
#         except Exception:
#             logger.warning("Non-JSON response from PawaPay for initiate-payment: %s", resp.text)

#         db = get_db()
#         db.execute("""
#             INSERT OR REPLACE INTO transactions
#             (depositId,status,amount,currency,phoneNumber,provider,
#              providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
#             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
#         """, (
#             deposit_id,
#             result.get("status", "PENDING"),
#             float(amount),
#             "ZMW",
#             phone,
#             None, None, None, None,
#             json.dumps(payload["metadata"]),
#             datetime.utcnow().isoformat(),
#             "payment",
#             None
#         ))
#         db.commit()
#         logger.info("initiate-payment: inserted depositId=%s status=%s", deposit_id, result.get("status", "PENDING"))
#         return jsonify({"depositId": deposit_id, **result}), 200

#     except Exception:
#         logger.exception("Payment initiation error")
#         return jsonify({"error": "Internal server error"}), 500

# # -------------------------
# # CALLBACK RECEIVER (upsert-safe for deposits and payouts)
# # -------------------------

# #Test 2 callback 2
# import sqlite3
# from flask import Flask, request, jsonify

# app = Flask(__name__)

# import sqlite3
# from flask import Flask, request, jsonify

# app = Flask(__name__)

# @app.route("/callback/deposit", methods=["POST"])
# def deposit_callback():
#     try:
#         data = request.get_json(force=True)
#         print("📩 Full callback data:", data)

#         deposit_id = data.get("depositId")
#         status = data.get("status", "PENDING").strip().upper()
#         amount = data.get("depositedAmount", 0)
#         metadata = data.get("metadata", {})
#         user_id = metadata.get("userId", "unknown")

#         if not deposit_id:
#             return jsonify({"error": "Missing depositId"}), 400

#         name_of_transaction = f"ZMW{amount} | {user_id} | {deposit_id}"

#         db = sqlite3.connect("estack.db")
#         db.row_factory = sqlite3.Row
#         cur = db.cursor()

#         # ✅ Use the correct table (same one as in /initiate)
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS estack_transactions (
#                 name_of_transaction TEXT NOT NULL,
#                 status TEXT NOT NULL
#             )
#         """)

#         # ✅ Check if transaction already exists
#         existing = cur.execute(
#             "SELECT name_of_transaction FROM estack_transactions WHERE name_of_transaction LIKE ?",
#             (f"%{deposit_id}%",)
#         ).fetchone()

#         if existing:
#             cur.execute(
#                 "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
#                 (status, f"%{deposit_id}%")
#             )
#             print(f"🔄 Updated transaction {deposit_id} → {status}")
#         else:
#             cur.execute(
#                 "INSERT INTO estack_transactions (name_of_transaction, status) VALUES (?, ?)",
#                 (name_of_transaction, status)
#             )
#             print(f"💾 Inserted new transaction {deposit_id} → {status}")

#         db.commit()
#         db.close()

#         return jsonify({"success": True, "deposit_id": deposit_id, "status": status}), 200

#     except Exception as e:
#         print("❌ Error in /callback/deposit:", e)
#         return jsonify({"error": str(e)}), 500

# -------------------------
# DEPOSIT STATUS / TRANSACTION LOOKUP
# # -------------------------
# @app.route("/deposit_status/<deposit_id>")
# def deposit_status(deposit_id):
#     db = get_db()
#     row = db.execute("SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ?", 
#                      (f"%{deposit_id}%",)).fetchone()
#     if not row:
#         return jsonify({"status": None, "message": "Deposit not found"}), 404
#     return jsonify(dict(row)), 200

# @app.route("/transactions/<deposit_id>")
# def get_transaction(deposit_id):
#     db = get_db()
#     row = db.execute("SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ?", 
#                      (f"%{deposit_id}%",)).fetchone()
#     if not row:
#         return jsonify({"error": "not found"}), 404
#     return jsonify(dict(row)), 200

@app.route("/deposit_status/<deposit_id>")
def deposit_status(deposit_id):
    db = get_db_sc()
    row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
    if not row:
        return jsonify({"status": None, "message": "Deposit not found"}), 404
    res = {k: row[k] for k in row.keys()}
    if res.get("metadata"):
        try:
            res["metadata"] = json.loads(res["metadata"])
        except:
            pass
    return jsonify(res), 200

@app.route("/transactions/<deposit_id>")
def get_transaction(deposit_id):
    db = get_db_sc()
    row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
    if not row:
        return jsonify({"error": "not found"}), 404
    res = {k: row[k] for k in row.keys()}
    if res.get("metadata"):
        try:
            res["metadata"] = json.loads(res["metadata"])
        except:
            pass
    return jsonify(res), 200

# -------------------------
# INVESTMENT ENDPOINTS (Using estack.db)
# -------------------------
@app.route("/api/investments/initiate", methods=["POST"])
def initiate_investment():
    try:
        data = request.json or {}
        phone = data.get("phone") or data.get("phoneNumber")
        amount = data.get("amount")
        correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
        currency = data.get("currency", "ZMW")
        user_id = data.get("user_id") or data.get("userId") or "unknown"

        if not phone or amount is None:
            return jsonify({"error": "Missing phone or amount"}), 400

        # Generate a unique deposit ID for this investment
        deposit_id = str(uuid.uuid4())
        customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Prepare payload for PawaPay (still sending live or test request)
        payload = {
            "depositId": deposit_id,
            "amount": str(amount),
            "currency": currency,
            "correspondent": correspondent,
            "payer": {"type": "MSISDN", "address": {"value": phone}},
            "customerTimestamp": customer_ts,
            "statementDescription": "Investment",
            "metadata": [
                {"fieldName": "purpose", "fieldValue": "investment"},
                {"fieldName": "userId", "fieldValue": str(user_id), "isPII": True},
            ],
        }

        headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
        resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)

        # Try decoding the response
        try:
            result = resp.json()
        except Exception:
            logger.error("PawaPay response not JSON: %s", resp.text)
            return jsonify({"error": "Invalid response from PawaPay"}), 502

        status = result.get("status", "PENDING")

        # Create readable name for transaction
        # e.g. "K500 | user_001 | DEP12345"
        name_of_transaction = f"{currency}{amount} | {user_id} | {deposit_id}"

        # Save to estack.db
        db = get_db()
        db.execute("""
            INSERT INTO estack_transactions (name_of_transaction, status)
            VALUES (?, ?)
        """, (name_of_transaction, status))
        db.commit()

        logger.info("💰 Investment initiated: %s (user_id=%s, status=%s)",
                    name_of_transaction, user_id, status)

        return jsonify({
            "message": "Investment initiated successfully",
            "depositId": deposit_id,
            "user_id": user_id,
            "amount": amount,
            "status": status
        }), 200

    except Exception as e:
        logger.exception("Investment initiation error")
        return jsonify({"error": str(e)}), 500

@app.route("/api/investments/user/<user_id>", methods=["GET"])
def get_user_investments(user_id):
    try:
        db = get_db()
        rows = db.execute("""
            SELECT name_of_transaction, status
            FROM estack_transactions
            WHERE name_of_transaction LIKE ?
            ORDER BY rowid DESC
        """, (f"%{user_id}%",)).fetchall()

        results = [{"name_of_transaction": r["name_of_transaction"], "status": r["status"]} for r in rows]
        return jsonify(results), 200

    except Exception as e:
        logger.exception("Error fetching user investments")
        return jsonify({"error": str(e)}), 500

@app.route("/api/investments/status/<deposit_id>", methods=["GET"])
def get_investment_status(deposit_id):
    try:
        db = sqlite3.connect("estack.db")
        db.row_factory = sqlite3.Row
        cur = db.cursor()

        # ✅ Match the same table name
        cur.execute("SELECT status FROM estack_transactions WHERE name_of_transaction LIKE ?", (f"%{deposit_id}%",))
        row = cur.fetchone()
        db.close()

        if row:
            return jsonify({"status": row["status"]}), 200
        else:
            return jsonify({"error": "Transaction not found"}), 404

    except Exception as e:
        print("Error in get_investment_status:", e)
        return jsonify({"error": str(e)}), 500

# # +++++++++++++++++++++++++++++++++++++++
# # Rerieving loans requests
# # +++++++++++++++++++++++++++++++++++++++
# @app.route("/api/loans/user/<user_id>", methods=["GET"])
# def get_user_loans(user_id):
#     try:
#         db = get_db()
#         rows = db.execute("""
#             SELECT name, status
#             FROM transactions
#             WHERE name LIKE ?
#             ORDER BY rowid DESC
#         """, (f"%{user_id}%",)).fetchall()

#         results = []
#         for r in rows:
#             name = r["name"]
#             status = r["status"]

#             # Try to split the stored name like: "ZMW500 | user_1 | some-loan-id"
#             parts = [p.strip() for p in name.split("|")]
#             amount, borrower_id, loan_id = parts if len(parts) == 3 else ("N/A", user_id, "N/A")

#             results.append({
#                 "loan_id": loan_id,
#                 "amount": amount,
#                 "status": status,
#                 "borrower_id": borrower_id
#             })

#         return jsonify(results), 200

#     except Exception as e:
#         logger.exception("Error fetching user loans")
#         return jsonify({"error": str(e)}), 500

#-----------------------------------
# GET PENDING REQUESTS
#----------------------------------
@app.route("/api/loans/pending", methods=["GET"])
def get_pending_loans():
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute("SELECT loanId, user_id, amount, interest, status, expected_return_date FROM loans WHERE status = ?", ("PENDING",))
    rows = cur.fetchall()
    conn.close()

    loans = []
    for row in rows:
        loans.append({
            "loanId": row[0],
            "user_id": row[1],
            "amount": row[2],
            "interest": row[3],
            "status": row[4],
            "expected_return_date": row[5]
        })

    return jsonify(loans), 200

# # -------------------------
# # DISBURSE LOAN (ADMIN ACTION)
# # -------------------------

# #TEST 4
# @app.route("/api/loans/disburse/<loan_id>", methods=["POST"])
# def disburse_loan(loan_id):
#     try:
#         db = get_db()  # ✅ Get the database connection
#         data = request.get_json() or {}
#         logger.info(f"Disbursing loan {loan_id} with data: {data}")

#         # ✅ Fetch loan details (fixed column name)
#         loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
#         if not loan:
#             return jsonify({"error": "Loan not found"}), 404

#         # ✅ Ensure loan is approved before disbursement
#         # if loan["status"] != "approved":
#         #     return jsonify({"error": "Loan is not approved for disbursement"}), 400

#         borrower_id = loan["user_id"]
#         amount = float(loan["amount"])

#         # ✅ Fetch borrower wallet
#         borrower_wallet = db.execute(
#             "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
#         ).fetchone()
        
#         if not borrower_wallet:
#             db.execute("""
#                 INSERT INTO wallets (user_id, balance, created_at, updated_at)
#                 VALUES (?, 0, ?, ?)
#             """, (borrower_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat()))
#             db.commit()
#             logger.info(f"✅ Created new wallet for borrower {borrower_id}")
        
#             borrower_wallet = db.execute(
#                 "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
#             ).fetchone()

#         borrower_balance = float(borrower_wallet["balance"])

#         # ✅ Credit borrower wallet
#         new_balance = borrower_balance + amount
#         db.execute(
#             "UPDATE wallets SET balance = ?, updated_at = ? WHERE user_id = ?",
#             (new_balance, datetime.utcnow().isoformat(), borrower_id)
#         )

#         # ✅ Mark loan as disbursed (fixed column name)
#         db.execute(
#             "UPDATE loans SET status = 'disbursed', disbursed_at = ? WHERE loanId = ?",
#             (datetime.utcnow().isoformat(), loan_id)
#         )

#         # ✅ Record the disbursement transaction
#         db.execute("""
#             INSERT INTO transactions (user_id, amount, type, status, reference, created_at, updated_at)
#             VALUES (?, ?, 'loan_disbursement', 'SUCCESS', ?, ?, ?)
#         """, (
#             borrower_id, amount, loan_id,
#             datetime.utcnow().isoformat(),
#             datetime.utcnow().isoformat()
#         ))

#         db.commit()

#         # ✅ Link this loan to one available investment
#         try:
#             # investment_row = db.execute("""
#             #     SELECT depositId FROM transactions
#             #     WHERE type = 'investment' AND status = 'ACTIVE'
#             #     ORDER BY received_at ASC LIMIT 1
#             # """).fetchone()
#             investment_row = db.execute("""
#                 SELECT reference FROM transactions
#                 WHERE type = 'investment' AND status = 'ACTIVE'
#                 ORDER BY created_at ASC LIMIT 1
#             """).fetchone()


#             if investment_row:
#                 # investment_id = investment_row["depositId"]
#                 investment_id = investment_row["reference"]


#                 # ✅ Mark that single investment as LOANED_OUT
#                 db.execute("""
#                     UPDATE transactions
#                     SET status = 'LOANED_OUT', updated_at = ?
#                     WHERE reference = ?

#                     # UPDATE transactions
#                     # SET status = 'LOANED_OUT', investment_id = ?, updated_at = ?
#                     # WHERE depositId = ?
#                 """, (loan_id, datetime.utcnow().isoformat(), investment_id))
#                 db.commit()

#                 # ✅ Notify the investor
#                 investor_row = db.execute("""
#                     SELECT user_id FROM transactions
#                     WHERE reference = ? AND type = 'investment'
#                     # SELECT user_id FROM transactions
#                     # WHERE depositId = ? AND type = 'investment'
#                 """, (investment_id,)).fetchone()
#                 # investment_row = db.execute("""
#                 #     SELECT reference FROM transactions
#                 #     WHERE type = 'investment' AND status = 'ACTIVE'
#                 #     ORDER BY created_at ASC LIMIT 1
#                 # """).fetchone()

#                 # ✅ Also mark investor's transaction as DISBURSED
#                 if loan["investment_id"]:
#                     db.execute("""
#                         UPDATE transactions
#                         SET status = 'DISBURSED', updated_at = ?
#                         WHERE depositId = ?
#                     """, (datetime.utcnow().isoformat(), loan["investment_id"]))
#                     logger.info(f"✅ Investor transaction {loan['investment_id']} marked as DISBURSED.")



#                 if investor_row and investor_row["user_id"]:
#                     notify_investor(
#                         investor_row["user_id"],
#                         f"Your investment {investment_id[:8]} has been loaned out to borrower {loan_id[:8]}."
#                     )

#                 logger.info(f"Investment {investment_id} linked to loan {loan_id}")
#             else:
#                 logger.warning("No available active investment found to link with this loan.")

#         except Exception as e:
#             logger.error(f"Error linking investment to loan {loan_id}: {e}")

#         return jsonify({
#             "message": f"Loan {loan_id} successfully disbursed",
#             "borrower_id": borrower_id,
#             "amount": amount,
#             "new_balance": new_balance
#         }), 200

#     except Exception as e:
#         logger.error(f"Error disbursing loan {loan_id}: {e}")
#         return jsonify({"error": str(e)}), 500

# # -------------------------
# # REJECT LOAN (ADMIN ACTION)
# # -------------------------
# @app.route("/api/loans/reject/<loan_id>", methods=["POST"])
# def reject_loan(loan_id):
#     admin_id = request.json.get("admin_id", "admin_default")
#     db = get_db()
#     loan = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
#     if not loan:
#         return jsonify({"error": "Loan not found"}), 404
#     if loan["status"] != "PENDING":
#         return jsonify({"error": f"Loan already {loan['status']}"}), 400

#     db.execute("UPDATE loans SET status='REJECTED', approved_by=? WHERE loanId=?", (admin_id, loan_id))
#     db.commit()

#     return jsonify({"loanId": loan_id, "status": "REJECTED"}), 200

# # OPTIONAL CODE CHECK NOTIFICATION 
# @app.route("/api/notifications/<user_id>", methods=["GET"])
# def get_notifications(user_id):
#     conn = sqlite3.connect(DATABASE)
#     conn.row_factory = sqlite3.Row
#     cur = conn.cursor()
#     cur.execute("SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC", (user_id,))
#     rows = cur.fetchall()
#     conn.close()
#     return jsonify([dict(row) for row in rows]), 200

# # -------------------------
# # RUN
# # -------------------------

# if __name__ == "__main__":
#     with app.app_context():
#         init_db()
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)







# from dotenv import load_dotenv
# load_dotenv()

# from flask import Flask, request, jsonify, g
# import os, logging, sqlite3, json, requests, uuid
# from datetime import datetime

# # -------------------------
# # API CONFIGURATION
# # -------------------------
# API_MODE = os.getenv("API_MODE", "sandbox")
# SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
# LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")

# API_TOKEN = LIVE_API_TOKEN if API_MODE == "live" else SANDBOX_API_TOKEN
# PAWAPAY_URL = (
#     "https://api.pawapay.io/deposits"
#     if API_MODE == "live"
#     else "https://api.sandbox.pawapay.io/deposits"
# )

# PAWAPAY_PAYOUT_URL = (
#     "https://api.pawapay.io/v2/payouts"
#     if API_MODE == "live"
#     else "https://api.sandbox.pawapay.io/v2/payouts"
# )

# # -------------------------
# # DATABASE
# # -------------------------
# DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")
# app = Flask(__name__)
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # JUST ADDED 1___________________________________________
# def notify_investor(user_id, message):
#     """
#     Notify investor of investment status change.
#     In real systems this could send email, SMS, or push.
#     For now, we just log and store in a notifications table.
#     """
#     try:
#         conn = sqlite3.connect(DATABASE)
#         cur = conn.cursor()
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS notifications (
#                 id INTEGER PRIMARY KEY AUTOINCREMENT,
#                 user_id TEXT,
#                 message TEXT,
#                 created_at TEXT
#             )
#         """)
#         conn.commit()

#         cur.execute("""
#             INSERT INTO notifications (user_id, message, created_at)
#             VALUES (?, ?, ?)
#         """, (user_id, message, datetime.utcnow().isoformat()))
#         conn.commit()
#         conn.close()

#         logger.info(f"📢 Notification sent to investor {user_id}: {message}")
#     except Exception as e:
#         logger.error(f"❌ Failed to notify investor {user_id}: {e}")

# def init_db():
#     """
#     Create the transactions and loans tables if missing and safely add any missing columns.
#     Also run a small backfill to populate 'type' and 'user_id' from metadata where possible.
#     """
#     conn = sqlite3.connect(DATABASE)
#     cur = conn.cursor()

#     # Create wallets table if not exists
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS wallets (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             user_id TEXT NOT NULL,
#             balance REAL DEFAULT 0,
#             currency TEXT DEFAULT 'ZMW',
#             updated_at TEXT,
#             created_at TEXT DEFAULT CURRENT_TIMESTAMP
#         )
#     """)
#     # =========================
#     # ✅ TRANSACTIONS TABLE
#     # =========================
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS transactions (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             depositId TEXT UNIQUE,
#             status TEXT,
#             amount REAL,
#             currency TEXT,
#             phoneNumber TEXT,
#             provider TEXT,
#             providerTransactionId TEXT,
#             failureCode TEXT,
#             failureMessage TEXT,
#             metadata TEXT,
#             received_at TEXT,
#             updated_at TEXT,
#             created_at TEXT,
#             type TEXT DEFAULT 'payment',
#             user_id TEXT,
#             investment_id TEXT,
#             reference TEXT  -- ✅ added
#         )
#     """)
#     conn.commit()

#     cur.execute("PRAGMA table_info(transactions)")
#     existing_cols = [r[1] for r in cur.fetchall()]

#     needed = {
#         "reference": "TEXT",
#         "phoneNumber": "TEXT",
#         "metadata": "TEXT",
#         "updated_at": "TEXT",
#         "created_at": "TEXT",
#         "type": "TEXT DEFAULT 'payment'",
#         "user_id": "TEXT",
#         "investment_id": "TEXT"
#     }

#     for col, coltype in needed.items():
#         if col not in existing_cols:
#             try:
#                 cur.execute(f"ALTER TABLE transactions ADD COLUMN {col} {coltype}")
#                 logger.info("Added column %s to transactions table", col)
#             except sqlite3.OperationalError:
#                 logger.warning("Could not add column %s (may already exist)", col)
#     conn.commit()

#         # =========================
#     # ✅ LOANS TABLE (matches code)
#     # =========================
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS loans (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             loanId TEXT UNIQUE,
#             user_id TEXT,
#             investment_id TEXT,
#             amount REAL,
#             interest REAL,
#             status TEXT,
#             expected_return_date TEXT,
#             created_at TEXT,
#             phone TEXT,
#             metadata TEXT
#         )
#     """)
#     conn.commit()

#     cur.execute("PRAGMA table_info(loans)")
#     existing_loan_cols = [r[1] for r in cur.fetchall()]

#     loan_needed = {
#         "loanId": "TEXT UNIQUE",
#         "user_id": "TEXT",
#         "investment_id": "TEXT",
#         "amount": "REAL",
#         "interest": "REAL",
#         "status": "TEXT",
#         "expected_return_date": "TEXT",
#         "created_at": "TEXT",
#         "phone": "TEXT",
#         "metadata": "TEXT"
#     }

#     for col, coltype in loan_needed.items():
#         if col not in existing_loan_cols:
#             try:
#                 cur.execute(f"ALTER TABLE loans ADD COLUMN {col} {coltype}")
#                 logger.info("Added column %s to loans table", col)
#             except sqlite3.OperationalError:
#                 logger.warning("Could not add column %s (may already exist)", col)

#     conn.commit()

#     # =========================
#     # ✅ BACKFILL TRANSACTIONS
#     # =========================
#     try:
#         cur.execute("SELECT depositId, metadata, type, user_id FROM transactions")
#         rows = cur.fetchall()
#         updates = []
#         for deposit_id, metadata, cur_type, cur_user in rows:
#             new_type = cur_type
#             new_user = cur_user
#             changed = False
#             if metadata:
#                 try:
#                     meta_obj = json.loads(metadata)
#                 except Exception:
#                     meta_obj = None

#                 if isinstance(meta_obj, list):
#                     for entry in meta_obj:
#                         if not isinstance(entry, dict):
#                             continue
#                         fn = str(entry.get("fieldName") or "").lower()
#                         fv = entry.get("fieldValue")
#                         if fn == "userid" and fv and not new_user:
#                             new_user = str(fv)
#                             changed = True
#                         if fn == "purpose" and isinstance(fv, str) and fv.lower() == "investment":
#                             if new_type != "investment":
#                                 new_type = "investment"
#                                 changed = True
#                 elif isinstance(meta_obj, dict):
#                     if "userId" in meta_obj and not new_user:
#                         new_user = str(meta_obj.get("userId"))
#                         changed = True
#                     purpose = meta_obj.get("purpose")
#                     if isinstance(purpose, str) and purpose.lower() == "investment":
#                         if new_type != "investment":
#                             new_type = "investment"
#                             changed = True

#             if new_type is None:
#                 new_type = "payment"

#             if changed or (cur_user is None and new_user is not None) or (cur_type is None and new_type):
#                 updates.append((new_user, new_type, deposit_id))

#         for u, t, dep in updates:
#             cur.execute("UPDATE transactions SET user_id = ?, type = ? WHERE depositId = ?", (u, t, dep))
#         if updates:
#             conn.commit()
#             logger.info("Backfilled %d transactions with user_id/type from metadata.", len(updates))
#     except Exception:
#         logger.exception("Error during migration/backfill pass")

#     conn.close()


# # ✅ Run safely within the Flask app context
# with app.app_context():
#     init_db()

# def get_db():
#     """
#     Return a DB connection scoped to the Flask request context.
#     Row factory is sqlite3.Row for dict-like rows.
#     """
#     db = getattr(g, "_database", None)
#     if db is None:
#         db = g._database = sqlite3.connect(DATABASE)
#         db.row_factory = sqlite3.Row
#     return db

# # -------------------------
# # LOANS TABLE INIT
# # -------------------------
# def init_loans_table():
#     conn = sqlite3.connect(DATABASE)
#     cur = conn.cursor()
#     cur.execute("""
#     CREATE TABLE IF NOT EXISTS loans (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         loanId TEXT UNIQUE,
#         user_id TEXT,
#         phone TEXT,                -- 🔹 NEW: borrower's phone number for payouts
#         investment_id TEXT,        -- 🔹 links this loan to an investment
#         amount REAL,
#         interest REAL,
#         status TEXT,               -- PENDING, APPROVED, DISAPPROVED, PAID
#         expected_return_date TEXT,
#         created_at TEXT,
#         approved_by TEXT
#     )
#     """)

#     conn.commit()
#     conn.close()
    
# with app.app_context():
#     init_loans_table()

# def migrate_loans_table():
#     db = get_db()
#     existing_columns = [col["name"] for col in db.execute("PRAGMA table_info(loans)").fetchall()]

#     # ✅ Ensure disbursed_at column exists
#     if "disbursed_at" not in existing_columns:
#         db.execute("ALTER TABLE loans ADD COLUMN disbursed_at TEXT")
#         db.commit()
#         print("✅ Added missing column: disbursed_at")

#     db.close()


# # ✅ run migrations safely once app starts
# with app.app_context():
#     init_db()
#     migrate_loans_table()


# # -------------------------
# # REQUEST A LOAN
# # # -------------------------

# @app.route("/api/loans/request", methods=["POST"])
# def request_loan():
#     data = request.json
#     user_id = data.get("user_id")
#     investment_id = data.get("investment_id")
#     amount = data.get("amount")
#     interest = data.get("interest", 5)
#     expected_return_date = data.get("expected_return_date")
#     phone = data.get("phone")  # <- NEW

#     if not user_id or not amount or not expected_return_date or not investment_id or not phone:
#         return jsonify({"error": "Missing required fields"}), 400

#     loanId = str(uuid.uuid4())
#     created_at = datetime.utcnow().isoformat()

#     conn = sqlite3.connect(DATABASE)
#     cur = conn.cursor()
#     cur.execute("""
#         INSERT INTO loans (loanId, user_id, investment_id, amount, interest, status, expected_return_date, created_at, phone)
#         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
#     """, (loanId, user_id, investment_id, amount, interest, "PENDING", expected_return_date, created_at, phone))
#     conn.commit()
#     conn.close()

#     return jsonify({"loanId": loanId, "status": "PENDING"}), 200

# # -------------------------
# # LIST PENDING LOANS (ADMIN VIEW)
# # -------------------------
# @app.route("/api/loans/pending", methods=["GET"])
# def pending_loans():
#     db = get_db()
#     rows = db.execute("SELECT * FROM loans WHERE status='PENDING' ORDER BY created_at DESC").fetchall()
#     results = [dict(row) for row in rows]
#     return jsonify(results), 200


# # -------------------------
# # APPROVE LOAN
# # # -------------------------
# @app.route("/api/loans/approve/<loan_id>", methods=["POST"])
# def approve_loan(loan_id):
#     try:
#         db = get_db()
#         admin_id = request.json.get("admin_id", "admin_default")

#         # ✅ Fetch loan by loanId
#         loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
#         if not loan:
#             return jsonify({"error": "Loan not found"}), 404

#         # ✅ Prevent double approval
#         if loan["status"] and loan["status"].upper() == "APPROVED":
#             return jsonify({"message": "Loan already approved"}), 200

#         now = datetime.utcnow().isoformat()

#         # ✅ Approve loan
#         db.execute("""
#             UPDATE loans
#             SET status = 'APPROVED',
#                 approved_by = ?,
#                 approved_at = ?,
#                 updated_at = ?
#             WHERE loanId = ?
#         """, (admin_id, now, now, loan_id))

#         # ✅ Update investor’s transaction using investment_id, not user_id
#         if loan["investment_id"]:
#             db.execute("""
#                 UPDATE transactions
#                 SET status = 'LOANED_OUT',
#                     updated_at = ?,
#                     failureMessage = 'Loan Approved',
#                     failureCode = 'LOAN'
#                 WHERE depositId = ?
#             """, (now, loan["investment_id"]))
#             logger.info(f"✅ Investor transaction {loan['investment_id']} marked as LOANED_OUT.")

#             # ✅ Notify investor
#             txn = db.execute("SELECT user_id FROM transactions WHERE depositId=?", (loan["investment_id"],)).fetchone()
#             if txn and txn["user_id"]:
#                 notify_investor(txn["user_id"], f"Your investment {loan['investment_id']} has been loaned out.")

#         db.commit()
#         return jsonify({"message": f"Loan {loan_id} approved and linked investor updated"}), 200

#     except Exception as e:
#         db.rollback()
#         logger.exception("Error approving loan")
#         return jsonify({"error": str(e)}), 500

# # @app.route("/api/loans/approve/<loan_id>", methods=["POST"])
# # def approve_loan(loan_id):
# #     try:
# #         db = get_db()
# #         admin_id = request.json.get("admin_id", "admin_default")

# #         # Check if loan exists
# #         loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
# #         if not loan:
# #             return jsonify({"error": "Loan not found"}), 404

# #         # Prevent double approval
# #         if loan["status"].upper() == "APPROVED":
# #             return jsonify({"message": "Loan already approved"}), 200

# #         # Update loan status
# #         db.execute("""
# #             UPDATE loans
# #             SET status = 'APPROVED',
# #                 approved_by = ?,
# #                 approved_at = ?,
# #                 updated_at = ?
# #             WHERE loanId = ?
# #         """, (
# #             admin_id,
# #             datetime.utcnow().isoformat(),
# #             datetime.utcnow().isoformat(),
# #             loan_id
# #         ))
# #         db.commit()

# #         # Update investor transaction if exists
# #         db.execute("""
# #             UPDATE transactions
# #             SET status = 'LOANED_OUT',
# #                 updated_at = ?,
# #                 metadata = COALESCE(metadata, ''),
# #                 failureMessage = 'Loan Approved',
# #                 failureCode = 'LOAN'
# #             WHERE user_id = ? AND type = 'investment'
# #         """, (datetime.utcnow().isoformat(), loan["user_id"]))
# #         db.commit()

# #         return jsonify({"message": f"Loan {loan_id} approved successfully"}), 200

# #     except Exception as e:
# #         db.rollback()
# #         return jsonify({"error": str(e)}), 500

# # -------------------------
# # DISAPPROVE LOAN
# # -------------------------
# @app.route("/api/loans/disapprove/<loan_id>", methods=["POST"])
# def disapprove_loan(loan_id):
#     db = get_db()
#     db.execute("UPDATE loans SET status='DISAPPROVED' WHERE loanId=?", (loan_id,))
#     db.commit()
#     return jsonify({"message": "Loan disapproved"}), 200


# # -------------------------
# # INVESTOR LOANS VIEW
# # -------------------------
# @app.route("/api/loans/user/<user_id>", methods=["GET"])
# def user_loans(user_id):
#     db = get_db()
#     rows = db.execute("SELECT * FROM loans WHERE user_id=? ORDER BY created_at DESC", (user_id,)).fetchall()
#     results = [dict(row) for row in rows]
#     return jsonify(results), 200

# @app.teardown_appcontext
# def close_connection(exception):
#     db = getattr(g, "_database", None)
#     if db is not None:
#         db.close()


# # -------------------------
# # HEALTH
# # -------------------------
# @app.route("/")
# def home():
#     return f"PawaPay Callback Receiver running ✅ (API_MODE={API_MODE})"


# # -------------------------
# # ORIGINAL PAYMENT ENDPOINTS
# # -------------------------
# @app.route("/initiate-payment", methods=["POST"])
# def initiate_payment():
#     try:
#         data = request.json
#         phone = data.get("phone")
#         amount = data.get("amount")
#         if not phone or not amount:
#             return jsonify({"error": "Missing phone or amount"}), 400

#         deposit_id = str(uuid.uuid4())
#         customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

#         payload = {
#             "depositId": deposit_id,
#             "amount": str(amount),
#             "currency": "ZMW",
#             "correspondent": "MTN_MOMO_ZMB",
#             "payer": {"type": "MSISDN", "address": {"value": phone}},
#             "customerTimestamp": customer_ts,
#             "statementDescription": "StudyCraftPay",
#             "metadata": [
#                 {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
#                 {"fieldName": "customerId", "fieldValue": phone, "isPII": True},
#             ],
#         }

#         headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
#         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)
#         result = {}
#         try:
#             result = resp.json()
#         except Exception:
#             logger.warning("Non-JSON response from PawaPay for initiate-payment: %s", resp.text)

#         db = get_db()
#         db.execute("""
#             INSERT OR REPLACE INTO transactions
#             (depositId,status,amount,currency,phoneNumber,provider,
#              providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
#             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
#         """, (
#             deposit_id,
#             result.get("status", "PENDING"),
#             float(amount),
#             "ZMW",
#             phone,
#             None, None, None, None,
#             json.dumps(payload["metadata"]),
#             datetime.utcnow().isoformat(),
#             "payment",
#             None
#         ))
#         db.commit()
#         logger.info("initiate-payment: inserted depositId=%s status=%s", deposit_id, result.get("status", "PENDING"))
#         return jsonify({"depositId": deposit_id, **result}), 200

#     except Exception:
#         logger.exception("Payment initiation error")
#         return jsonify({"error": "Internal server error"}), 500

# # -------------------------
# # CALLBACK RECEIVER (upsert-safe for deposits and payouts)
# # -------------------------

# #Test 2 callback 2
# @app.route("/callback/deposit", methods=["POST"])
# def deposit_callback():
#     try:
#         data = request.get_json(force=True)

#         # Determine if deposit or payout
#         deposit_id = data.get("depositId")
#         payout_id = data.get("payoutId")

#         if not deposit_id and not payout_id:
#             return jsonify({"error": "Missing depositId/payoutId"}), 400

#         txn_type = "payment" if deposit_id else "payout"
#         txn_id = deposit_id or payout_id

#         # Amount, status, currency
#         status = data.get("status")
#         amount = data.get("amount")
#         currency = data.get("currency")

#         # Phone & provider
#         if txn_type == "payment":
#             phone = data.get("payer", {}).get("accountDetails", {}).get("phoneNumber")
#             provider = data.get("payer", {}).get("accountDetails", {}).get("provider")
#         else:  # payout
#             phone = data.get("recipient", {}).get("accountDetails", {}).get("phoneNumber")
#             provider = data.get("recipient", {}).get("accountDetails", {}).get("provider")

#         provider_txn = data.get("providerTransactionId")
#         failure_code = data.get("failureReason", {}).get("failureCode")
#         failure_message = data.get("failureReason", {}).get("failureMessage")
#         metadata_obj = data.get("metadata")

#         user_id = None
#         loan_id = None
#         if metadata_obj:
#             if isinstance(metadata_obj, dict):
#                 user_id = metadata_obj.get("userId")
#                 loan_id = metadata_obj.get("loanId")
#             elif isinstance(metadata_obj, list):
#                 for entry in metadata_obj:
#                     if isinstance(entry, dict):
#                         if entry.get("fieldName") == "userId":
#                             user_id = entry.get("fieldValue")
#                         if entry.get("fieldName") == "loanId":
#                             loan_id = entry.get("fieldValue")

#         db = get_db()
#         existing = db.execute("SELECT * FROM transactions WHERE depositId=? OR depositId=?",
#                               (deposit_id, payout_id)).fetchone()
#         now_iso = datetime.utcnow().isoformat()
#         metadata_str = json.dumps(metadata_obj) if metadata_obj else None

#         if existing:
#             db.execute("""
#                 UPDATE transactions
#                 SET
#                     status = COALESCE(?, status),
#                     amount = COALESCE(?, amount),
#                     currency = COALESCE(?, currency),
#                     phoneNumber = COALESCE(?, phoneNumber),
#                     provider = COALESCE(?, provider),
#                     providerTransactionId = COALESCE(?, providerTransactionId),
#                     failureCode = COALESCE(?, failureCode),
#                     failureMessage = COALESCE(?, failureMessage),
#                     metadata = COALESCE(?, metadata),
#                     updated_at = ?,
#                     user_id = COALESCE(?, user_id)
#                 WHERE depositId = ? OR depositId = ?
#             """, (
#                 status,
#                 float(amount) if amount else None,
#                 currency,
#                 phone,
#                 provider,
#                 provider_txn,
#                 failure_code,
#                 failure_message,
#                 metadata_str,
#                 now_iso,
#                 user_id,
#                 deposit_id,
#                 payout_id
#             ))
#         else:
#             db.execute("""
#                 INSERT INTO transactions
#                 (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId,
#                  failureCode, failureMessage, metadata, received_at, updated_at, type, user_id)
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#             """, (
#                 txn_id,
#                 status,
#                 float(amount) if amount else None,
#                 currency,
#                 phone,
#                 provider,
#                 provider_txn,
#                 failure_code,
#                 failure_message,
#                 metadata_str,
#                 now_iso,
#                 now_iso,
#                 txn_type,
#                 user_id
#             ))

#         # Update loan if payout succeeded
#         # Update loan if payout succeeded
#         if txn_type == "payout" and loan_id and status in ("COMPLETED", "SUCCESS", "PAYMENT_COMPLETED"):
#             db.execute("UPDATE loans SET status=? WHERE loanId=?", (status, loan_id))

#             # Find user and notify repayment
#             loan_row = db.execute("SELECT user_id FROM loans WHERE loanId=?", (loan_id,)).fetchone()
#             if loan_row and loan_row["user_id"]:
#                 notify_investor(
#                     loan_row["user_id"],
#                     f"Loan {loan_id[:8]} has been successfully repaid."
#                 )

#         # if txn_type == "payout" and loan_id and status in ("COMPLETED", "SUCCESS", "PAYMENT_COMPLETED"):
#         #     db.execute("UPDATE loans SET status=? WHERE loanId=?", (status, loan_id))

#         db.commit()
#         return jsonify({"received": True}), 200

#     except Exception:
#         logger.exception("Callback error")
#         return jsonify({"error": "Internal server error"}), 500
# # @app.route("/callback/deposit", methods=["POST"])
# # def deposit_callback():
# #     try:
# #         data = request.get_json(force=True)

# #         # Determine if deposit or payout
# #         deposit_id = data.get("depositId")
# #         payout_id = data.get("payoutId")

# #         if not deposit_id and not payout_id:
# #             return jsonify({"error": "Missing depositId/payoutId"}), 400

# #         txn_type = "payment" if deposit_id else "payout"
# #         txn_id = deposit_id or payout_id

# #         # Amount, status, currency
# #         status = data.get("status")
# #         amount = data.get("amount")
# #         currency = data.get("currency")

# #         # Phone & provider
# #         if txn_type == "payment":
# #             phone = data.get("payer", {}).get("accountDetails", {}).get("phoneNumber")
# #             provider = data.get("payer", {}).get("accountDetails", {}).get("provider")
# #         else:  # payout
# #             phone = data.get("recipient", {}).get("accountDetails", {}).get("phoneNumber")
# #             provider = data.get("recipient", {}).get("accountDetails", {}).get("provider")

# #         provider_txn = data.get("providerTransactionId")
# #         failure_code = data.get("failureReason", {}).get("failureCode")
# #         failure_message = data.get("failureReason", {}).get("failureMessage")
# #         metadata_obj = data.get("metadata")

# #         user_id = None
# #         loan_id = None
# #         if metadata_obj:
# #             if isinstance(metadata_obj, dict):
# #                 user_id = metadata_obj.get("userId")
# #                 loan_id = metadata_obj.get("loanId")
# #             elif isinstance(metadata_obj, list):
# #                 for entry in metadata_obj:
# #                     if isinstance(entry, dict):
# #                         if entry.get("fieldName") == "userId":
# #                             user_id = entry.get("fieldValue")
# #                         if entry.get("fieldName") == "loanId":
# #                             loan_id = entry.get("fieldValue")

# #         db = get_db()
# #         existing = db.execute("SELECT * FROM transactions WHERE depositId=? OR depositId=?",
# #                               (deposit_id, payout_id)).fetchone()
# #         now_iso = datetime.utcnow().isoformat()
# #         metadata_str = json.dumps(metadata_obj) if metadata_obj else None

# #         if existing:
# #             db.execute("""
# #                 UPDATE transactions
# #                 SET
# #                     status = COALESCE(?, status),
# #                     amount = COALESCE(?, amount),
# #                     currency = COALESCE(?, currency),
# #                     phoneNumber = COALESCE(?, phoneNumber),
# #                     provider = COALESCE(?, provider),
# #                     providerTransactionId = COALESCE(?, providerTransactionId),
# #                     failureCode = COALESCE(?, failureCode),
# #                     failureMessage = COALESCE(?, failureMessage),
# #                     metadata = COALESCE(?, metadata),
# #                     updated_at = ?,
# #                     user_id = COALESCE(?, user_id)
# #                 WHERE depositId = ? OR depositId = ?
# #             """, (
# #                 status,
# #                 float(amount) if amount else None,
# #                 currency,
# #                 phone,
# #                 provider,
# #                 provider_txn,
# #                 failure_code,
# #                 failure_message,
# #                 metadata_str,
# #                 now_iso,
# #                 user_id,
# #                 deposit_id,
# #                 payout_id
# #             ))
# #         else:
# #             db.execute("""
# #                 INSERT INTO transactions
# #                 (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId,
# #                  failureCode, failureMessage, metadata, received_at, updated_at, type, user_id)
# #                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
# #             """, (
# #                 txn_id,
# #                 status,
# #                 float(amount) if amount else None,
# #                 currency,
# #                 phone,
# #                 provider,
# #                 provider_txn,
# #                 failure_code,
# #                 failure_message,
# #                 metadata_str,
# #                 now_iso,
# #                 now_iso,
# #                 txn_type,
# #                 user_id
# #             ))

# #         # Update loan if payout succeeded
# #         # Update loan if payout succeeded
# #         if txn_type == "payout" and loan_id and status in ("COMPLETED", "SUCCESS", "PAYMENT_COMPLETED"):
# #             db.execute("UPDATE loans SET status=? WHERE loanId=?", (status, loan_id))

# #             # Find user and notify repayment
# #             loan_row = db.execute("SELECT user_id FROM loans WHERE loanId=?", (loan_id,)).fetchone()
# #             if loan_row and loan_row["user_id"]:
# #                 notify_investor(
# #                     loan_row["user_id"],
# #                     f"Loan {loan_id[:8]} has been successfully repaid."
# #                 )

# #         # if txn_type == "payout" and loan_id and status in ("COMPLETED", "SUCCESS", "PAYMENT_COMPLETED"):
# #         #     db.execute("UPDATE loans SET status=? WHERE loanId=?", (status, loan_id))

# #         db.commit()
# #         return jsonify({"received": True}), 200

# #     except Exception:
# #         logger.exception("Callback error")
# #         return jsonify({"error": "Internal server error"}), 500

# # -------------------------
# # DEPOSIT STATUS / TRANSACTION LOOKUP
# # -------------------------
# @app.route("/deposit_status/<deposit_id>")
# def deposit_status(deposit_id):
#     db = get_db()
#     row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
#     if not row:
#         return jsonify({"status": None, "message": "Deposit not found"}), 404
#     res = {k: row[k] for k in row.keys()}
#     if res.get("metadata"):
#         try:
#             res["metadata"] = json.loads(res["metadata"])
#         except:
#             pass
#     return jsonify(res), 200


# @app.route("/transactions/<deposit_id>")
# def get_transaction(deposit_id):
#     db = get_db()
#     row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
#     if not row:
#         return jsonify({"error": "not found"}), 404
#     res = {k: row[k] for k in row.keys()}
#     if res.get("metadata"):
#         try:
#             res["metadata"] = json.loads(res["metadata"])
#         except:
#             pass
#     return jsonify(res), 200


# # -------------------------
# # INVESTMENT ENDPOINTS
# # -------------------------
# @app.route("/api/investments/initiate", methods=["POST"])
# def initiate_investment():
#     try:
#         data = request.json or {}
#         # Support both "phone" and "phoneNumber" keys from different clients
#         phone = data.get("phone") or data.get("phoneNumber")
#         amount = data.get("amount")
#         correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
#         currency = data.get("currency", "ZMW")
#         # prefer explicit user_id, but don't crash if missing
#         user_id = data.get("user_id") or data.get("userId") or "unknown"

#         if not phone or amount is None:
#             return jsonify({"error": "Missing phone or amount"}), 400

#         deposit_id = str(uuid.uuid4())
#         customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

#         payload = {
#             "depositId": deposit_id,
#             "amount": str(amount),
#             "currency": currency,
#             "correspondent": correspondent,
#             "payer": {"type": "MSISDN", "address": {"value": phone}},
#             "customerTimestamp": customer_ts,
#             "statementDescription": "Investment",
#             "metadata": [
#                 {"fieldName": "purpose", "fieldValue": "investment"},
#                 {"fieldName": "userId", "fieldValue": str(user_id), "isPII": True},
#             ],
#         }

#         headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
#         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)

#         try:
#             result = resp.json()
#         except Exception:
#             logger.error("PawaPay response not JSON: %s", resp.text)
#             return jsonify({"error": "Invalid response from PawaPay"}), 502

#         status = result.get("status", "PENDING")

#         db = get_db()
#         # Insert a new investment record (depositId will be unique)
#         db.execute("""
#             INSERT OR REPLACE INTO transactions
#             (depositId,status,amount,currency,phoneNumber,provider,
#              providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
#             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
#         """, (
#             deposit_id,
#             status,
#             float(amount),
#             currency,
#             phone,
#             None, None, None, None,
#             json.dumps(payload["metadata"]),
#             datetime.utcnow().isoformat(),
#             "investment",
#             user_id
#         ))
#         db.commit()
#         logger.info("initiate_investment: inserted depositId=%s user_id=%s amount=%s status=%s",
#                     deposit_id, user_id, amount, status)

#         return jsonify({"depositId": deposit_id, "status": status}), 200

#     except Exception as e:
#         logger.exception("Investment initiation error")
#         return jsonify({"error": str(e)}), 500


# @app.route("/api/investments/user/<user_id>", methods=["GET"])
# def get_user_investments(user_id):
#     """
#     Return investments for a user. We select type='investment' and the exact user_id column.
#     This returns a list of rows (may be empty).
#     """
#     db = get_db()
#     rows = db.execute(
#         "SELECT * FROM transactions WHERE type='investment' AND user_id=? ORDER BY received_at DESC",
#         (user_id,)
#     ).fetchall()

#     results = []
#     for row in rows:
#         res = {k: row[k] for k in row.keys()}
#         if res.get("metadata"):
#             try:
#                 res["metadata"] = json.loads(res["metadata"])
#             except:
#                 pass
#         results.append(res)

#     return jsonify(results), 200


# # -------------------------
# # SAMPLE INVESTMENT ROUTE (handy for testing)
# # -------------------------
# @app.route("/sample-investment", methods=["POST"])
# def add_sample():
#     """Add a test investment to verify DB works"""
#     try:
#         db = get_db()
#         deposit_id = str(uuid.uuid4())
#         payload_metadata = [{"fieldName": "purpose", "fieldValue": "investment"},
#                             {"fieldName": "userId", "fieldValue": "user_1"}]
#         received_at = datetime.utcnow().isoformat()
#         db.execute("""
#             INSERT INTO transactions
#             (depositId,status,amount,currency,phoneNumber,metadata,received_at,type,user_id)
#             VALUES (?,?,?,?,?,?,?,?,?)
#         """, (
#             deposit_id,
#             "SUCCESS",
#             1000.0,
#             "ZMW",
#             "0965123456",
#             json.dumps(payload_metadata),
#             received_at,
#             "investment",
#             "user_1"
#         ))
#         db.commit()
#         logger.info("Added sample investment depositId=%s", deposit_id)
#         return jsonify({"message":"Sample investment added","depositId":deposit_id}), 200
#     except Exception as e:
#         logger.exception("Failed to insert sample")
#         return jsonify({"error": str(e)}), 500


# # -------------------------
# # OPTIONAL: debug route to see all transactions (helpful during testing)
# # -------------------------
# @app.route("/debug/transactions", methods=["GET"])
# def debug_transactions():
#     db = get_db()
#     rows = db.execute("SELECT * FROM transactions ORDER BY received_at DESC").fetchall()
#     results = []
#     for row in rows:
#         res = {k: row[k] for k in row.keys()}
#         if res.get("metadata"):
#             try:
#                 res["metadata"] = json.loads(res["metadata"])
#             except:
#                 pass
#         results.append(res)
#     return jsonify(results), 200
    
# #-----------------------------------
# # GET PENDING REQUESTS
# #----------------------------------

# @app.route("/api/loans/pending", methods=["GET"])
# def get_pending_loans():
#     conn = sqlite3.connect(DATABASE)
#     cur = conn.cursor()
#     cur.execute("SELECT loanId, user_id, amount, interest, status, expected_return_date FROM loans WHERE status = ?", ("PENDING",))
#     rows = cur.fetchall()
#     conn.close()

#     loans = []
#     for row in rows:
#         loans.append({
#             "loanId": row[0],
#             "user_id": row[1],
#             "amount": row[2],
#             "interest": row[3],
#             "status": row[4],
#             "expected_return_date": row[5]
#         })

#     return jsonify(loans), 200

# # -------------------------
# # DISBURSE LOAN (ADMIN ACTION)
# # -------------------------

# #TEST 4
# @app.route("/api/loans/disburse/<loan_id>", methods=["POST"])
# def disburse_loan(loan_id):
#     try:
#         db = get_db()  # ✅ Get the database connection
#         data = request.get_json() or {}
#         logger.info(f"Disbursing loan {loan_id} with data: {data}")

#         # ✅ Fetch loan details (fixed column name)
#         loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
#         if not loan:
#             return jsonify({"error": "Loan not found"}), 404

#         # ✅ Ensure loan is approved before disbursement
#         # if loan["status"] != "approved":
#         #     return jsonify({"error": "Loan is not approved for disbursement"}), 400

#         borrower_id = loan["user_id"]
#         amount = float(loan["amount"])

#         # ✅ Fetch borrower wallet
#         borrower_wallet = db.execute(
#             "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
#         ).fetchone()
        
#         if not borrower_wallet:
#             db.execute("""
#                 INSERT INTO wallets (user_id, balance, created_at, updated_at)
#                 VALUES (?, 0, ?, ?)
#             """, (borrower_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat()))
#             db.commit()
#             logger.info(f"✅ Created new wallet for borrower {borrower_id}")
        
#             borrower_wallet = db.execute(
#                 "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
#             ).fetchone()

#         borrower_balance = float(borrower_wallet["balance"])

#         # ✅ Credit borrower wallet
#         new_balance = borrower_balance + amount
#         db.execute(
#             "UPDATE wallets SET balance = ?, updated_at = ? WHERE user_id = ?",
#             (new_balance, datetime.utcnow().isoformat(), borrower_id)
#         )

#         # ✅ Mark loan as disbursed (fixed column name)
#         db.execute(
#             "UPDATE loans SET status = 'disbursed', disbursed_at = ? WHERE loanId = ?",
#             (datetime.utcnow().isoformat(), loan_id)
#         )

#         # ✅ Record the disbursement transaction
#         db.execute("""
#             INSERT INTO transactions (user_id, amount, type, status, reference, created_at, updated_at)
#             VALUES (?, ?, 'loan_disbursement', 'SUCCESS', ?, ?, ?)
#         """, (
#             borrower_id, amount, loan_id,
#             datetime.utcnow().isoformat(),
#             datetime.utcnow().isoformat()
#         ))

#         db.commit()

#         # ✅ Link this loan to one available investment
#         try:
#             # investment_row = db.execute("""
#             #     SELECT depositId FROM transactions
#             #     WHERE type = 'investment' AND status = 'ACTIVE'
#             #     ORDER BY received_at ASC LIMIT 1
#             # """).fetchone()
#             investment_row = db.execute("""
#                 SELECT reference FROM transactions
#                 WHERE type = 'investment' AND status = 'ACTIVE'
#                 ORDER BY created_at ASC LIMIT 1
#             """).fetchone()


#             if investment_row:
#                 # investment_id = investment_row["depositId"]
#                 investment_id = investment_row["reference"]


#                 # ✅ Mark that single investment as LOANED_OUT
#                 db.execute("""
#                     UPDATE transactions
#                     SET status = 'LOANED_OUT', updated_at = ?
#                     WHERE reference = ?

#                     # UPDATE transactions
#                     # SET status = 'LOANED_OUT', investment_id = ?, updated_at = ?
#                     # WHERE depositId = ?
#                 """, (loan_id, datetime.utcnow().isoformat(), investment_id))
#                 db.commit()

#                 # ✅ Notify the investor
#                 investor_row = db.execute("""
#                     SELECT user_id FROM transactions
#                     WHERE reference = ? AND type = 'investment'
#                     # SELECT user_id FROM transactions
#                     # WHERE depositId = ? AND type = 'investment'
#                 """, (investment_id,)).fetchone()
#                 # investment_row = db.execute("""
#                 #     SELECT reference FROM transactions
#                 #     WHERE type = 'investment' AND status = 'ACTIVE'
#                 #     ORDER BY created_at ASC LIMIT 1
#                 # """).fetchone()

#                 # ✅ Also mark investor's transaction as DISBURSED
#                 if loan["investment_id"]:
#                     db.execute("""
#                         UPDATE transactions
#                         SET status = 'DISBURSED', updated_at = ?
#                         WHERE depositId = ?
#                     """, (datetime.utcnow().isoformat(), loan["investment_id"]))
#                     logger.info(f"✅ Investor transaction {loan['investment_id']} marked as DISBURSED.")



#                 if investor_row and investor_row["user_id"]:
#                     notify_investor(
#                         investor_row["user_id"],
#                         f"Your investment {investment_id[:8]} has been loaned out to borrower {loan_id[:8]}."
#                     )

#                 logger.info(f"Investment {investment_id} linked to loan {loan_id}")
#             else:
#                 logger.warning("No available active investment found to link with this loan.")

#         except Exception as e:
#             logger.error(f"Error linking investment to loan {loan_id}: {e}")

#         return jsonify({
#             "message": f"Loan {loan_id} successfully disbursed",
#             "borrower_id": borrower_id,
#             "amount": amount,
#             "new_balance": new_balance
#         }), 200

#     except Exception as e:
#         logger.error(f"Error disbursing loan {loan_id}: {e}")
#         return jsonify({"error": str(e)}), 500

# # -------------------------
# # REJECT LOAN (ADMIN ACTION)
# # -------------------------
# @app.route("/api/loans/reject/<loan_id>", methods=["POST"])
# def reject_loan(loan_id):
#     admin_id = request.json.get("admin_id", "admin_default")
#     db = get_db()
#     loan = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
#     if not loan:
#         return jsonify({"error": "Loan not found"}), 404
#     if loan["status"] != "PENDING":
#         return jsonify({"error": f"Loan already {loan['status']}"}), 400

#     db.execute("UPDATE loans SET status='REJECTED', approved_by=? WHERE loanId=?", (admin_id, loan_id))
#     db.commit()

#     return jsonify({"loanId": loan_id, "status": "REJECTED"}), 200

# # OPTIONAL CODE CHECK NOTIFICATION 
# @app.route("/api/notifications/<user_id>", methods=["GET"])
# def get_notifications(user_id):
#     conn = sqlite3.connect(DATABASE)
#     conn.row_factory = sqlite3.Row
#     cur = conn.cursor()
#     cur.execute("SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC", (user_id,))
#     rows = cur.fetchall()
#     conn.close()
#     return jsonify([dict(row) for row in rows]), 200

# # -------------------------
# # RUN
# # -------------------------

# if __name__ == "__main__":
#     with app.app_context():
#         init_db()
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)






    
# # from dotenv import load_dotenv
# # load_dotenv()

# # # 3
# # from flask import Flask, request, jsonify, g
# # import os, logging, sqlite3, json, requests, uuid
# # from datetime import datetime

# # import os
# # import dropbox
# # from flask_cors import CORS
# # from database_backup import download_db, upload_db  # ✅ Dropbox sync helpers

# # app = Flask(__name__)
# # CORS(app)

# # # ============================================================
# # #  🔹 Dropbox Auto Sync Section
# # # ============================================================

# # # Download the latest database on server startup
# # print("⏬ Checking Dropbox for latest estack.db...")
# # download_db()

# # def get_db():
# #     """Connect to SQLite database"""
# #     db = sqlite3.connect("estack.db", check_same_thread=False)
# #     db.row_factory = sqlite3.Row
# #     return db

# # # -------------------------
# # # API CONFIGURATION
# # # -------------------------
# # API_MODE = os.getenv("API_MODE", "sandbox")
# # SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
# # LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")

# # API_TOKEN = LIVE_API_TOKEN if API_MODE == "live" else SANDBOX_API_TOKEN
# # PAWAPAY_URL = (
# #     "https://api.pawapay.io/deposits"
# #     if API_MODE == "live"
# #     else "https://api.sandbox.pawapay.io/deposits"
# # )

# # PAWAPAY_PAYOUT_URL = (
# #     "https://api.pawapay.io/v2/payouts"
# #     if API_MODE == "live"
# #     else "https://api.sandbox.pawapay.io/v2/payouts"
# # )

# # # -------------------------
# # # DATABASE
# # # -------------------------
# # DATABASE = os.path.join(os.path.dirname(__file__), "transactions.db")
# # app = Flask(__name__)
# # logging.basicConfig(level=logging.INFO)
# # logger = logging.getLogger(__name__)

# # # JUST ADDED 1___________________________________________
# # def notify_investor(user_id, message):
# #     """
# #     Notify investor of investment status change.
# #     In real systems this could send email, SMS, or push.
# #     For now, we just log and store in a notifications table.
# #     """
# #     try:
# #         conn = sqlite3.connect(DATABASE)
# #         cur = conn.cursor()
# #         cur.execute("""
# #             CREATE TABLE IF NOT EXISTS notifications (
# #                 id INTEGER PRIMARY KEY AUTOINCREMENT,
# #                 user_id TEXT,
# #                 message TEXT,
# #                 created_at TEXT
# #             )
# #         """)
# #         conn.commit()

# #         cur.execute("""
# #             INSERT INTO notifications (user_id, message, created_at)
# #             VALUES (?, ?, ?)
# #         """, (user_id, message, datetime.utcnow().isoformat()))
# #         conn.commit()
# #         conn.close()

# #         logger.info(f"📢 Notification sent to investor {user_id}: {message}")
# #     except Exception as e:
# #         logger.error(f"❌ Failed to notify investor {user_id}: {e}")

# # def init_db():
# #     """
# #     Create the transactions and loans tables if missing and safely add any missing columns.
# #     Also run a small backfill to populate 'type' and 'user_id' from metadata where possible.
# #     """
# #     conn = sqlite3.connect(DATABASE)
# #     cur = conn.cursor()

# #     # Create wallets table if not exists
# #     cur.execute("""
# #         CREATE TABLE IF NOT EXISTS wallets (
# #             id INTEGER PRIMARY KEY AUTOINCREMENT,
# #             user_id TEXT NOT NULL,
# #             balance REAL DEFAULT 0,
# #             currency TEXT DEFAULT 'ZMW',
# #             updated_at TEXT,
# #             created_at TEXT DEFAULT CURRENT_TIMESTAMP
# #         )
# #     """)
# #     # =========================
# #     # ✅ TRANSACTIONS TABLE
# #     # =========================
# #     cur.execute("""
# #         CREATE TABLE IF NOT EXISTS transactions (
# #             id INTEGER PRIMARY KEY AUTOINCREMENT,
# #             depositId TEXT UNIQUE,
# #             status TEXT,
# #             amount REAL,
# #             currency TEXT,
# #             phoneNumber TEXT,
# #             provider TEXT,
# #             providerTransactionId TEXT,
# #             failureCode TEXT,
# #             failureMessage TEXT,
# #             metadata TEXT,
# #             received_at TEXT,
# #             updated_at TEXT,
# #             created_at TEXT,
# #             type TEXT DEFAULT 'payment',
# #             user_id TEXT,
# #             investment_id TEXT,
# #             reference TEXT  -- ✅ added
# #         )
# #     """)
# #     conn.commit()

# #     cur.execute("PRAGMA table_info(transactions)")
# #     existing_cols = [r[1] for r in cur.fetchall()]

# #     needed = {
# #         "reference": "TEXT",
# #         "phoneNumber": "TEXT",
# #         "metadata": "TEXT",
# #         "updated_at": "TEXT",
# #         "created_at": "TEXT",
# #         "type": "TEXT DEFAULT 'payment'",
# #         "user_id": "TEXT",
# #         "investment_id": "TEXT"
# #     }

# #     for col, coltype in needed.items():
# #         if col not in existing_cols:
# #             try:
# #                 cur.execute(f"ALTER TABLE transactions ADD COLUMN {col} {coltype}")
# #                 logger.info("Added column %s to transactions table", col)
# #             except sqlite3.OperationalError:
# #                 logger.warning("Could not add column %s (may already exist)", col)
# #     conn.commit()

# #         # =========================
# #     # ✅ LOANS TABLE (matches code)
# #     # =========================
# #     cur.execute("""
# #         CREATE TABLE IF NOT EXISTS loans (
# #             id INTEGER PRIMARY KEY AUTOINCREMENT,
# #             loanId TEXT UNIQUE,
# #             user_id TEXT,
# #             investment_id TEXT,
# #             amount REAL,
# #             interest REAL,
# #             status TEXT,
# #             expected_return_date TEXT,
# #             created_at TEXT,
# #             phone TEXT,
# #             metadata TEXT
# #         )
# #     """)
# #     conn.commit()

# #     cur.execute("PRAGMA table_info(loans)")
# #     existing_loan_cols = [r[1] for r in cur.fetchall()]

# #     loan_needed = {
# #         "loanId": "TEXT UNIQUE",
# #         "user_id": "TEXT",
# #         "investment_id": "TEXT",
# #         "amount": "REAL",
# #         "interest": "REAL",
# #         "status": "TEXT",
# #         "expected_return_date": "TEXT",
# #         "created_at": "TEXT",
# #         "phone": "TEXT",
# #         "metadata": "TEXT"
# #     }

# #     for col, coltype in loan_needed.items():
# #         if col not in existing_loan_cols:
# #             try:
# #                 cur.execute(f"ALTER TABLE loans ADD COLUMN {col} {coltype}")
# #                 logger.info("Added column %s to loans table", col)
# #             except sqlite3.OperationalError:
# #                 logger.warning("Could not add column %s (may already exist)", col)

# #     conn.commit()

# #     # =========================
# #     # ✅ BACKFILL TRANSACTIONS
# #     # =========================
# #     try:
# #         cur.execute("SELECT depositId, metadata, type, user_id FROM transactions")
# #         rows = cur.fetchall()
# #         updates = []
# #         for deposit_id, metadata, cur_type, cur_user in rows:
# #             new_type = cur_type
# #             new_user = cur_user
# #             changed = False
# #             if metadata:
# #                 try:
# #                     meta_obj = json.loads(metadata)
# #                 except Exception:
# #                     meta_obj = None

# #                 if isinstance(meta_obj, list):
# #                     for entry in meta_obj:
# #                         if not isinstance(entry, dict):
# #                             continue
# #                         fn = str(entry.get("fieldName") or "").lower()
# #                         fv = entry.get("fieldValue")
# #                         if fn == "userid" and fv and not new_user:
# #                             new_user = str(fv)
# #                             changed = True
# #                         if fn == "purpose" and isinstance(fv, str) and fv.lower() == "investment":
# #                             if new_type != "investment":
# #                                 new_type = "investment"
# #                                 changed = True
# #                 elif isinstance(meta_obj, dict):
# #                     if "userId" in meta_obj and not new_user:
# #                         new_user = str(meta_obj.get("userId"))
# #                         changed = True
# #                     purpose = meta_obj.get("purpose")
# #                     if isinstance(purpose, str) and purpose.lower() == "investment":
# #                         if new_type != "investment":
# #                             new_type = "investment"
# #                             changed = True

# #             if new_type is None:
# #                 new_type = "payment"

# #             if changed or (cur_user is None and new_user is not None) or (cur_type is None and new_type):
# #                 updates.append((new_user, new_type, deposit_id))

# #         for u, t, dep in updates:
# #             cur.execute("UPDATE transactions SET user_id = ?, type = ? WHERE depositId = ?", (u, t, dep))
# #         if updates:
# #             conn.commit()
# #             logger.info("Backfilled %d transactions with user_id/type from metadata.", len(updates))
# #     except Exception:
# #         logger.exception("Error during migration/backfill pass")

# #     conn.close()


# # # ✅ Run safely within the Flask app context
# # with app.app_context():
# #     init_db()

# # def get_db():
# #     """
# #     Return a DB connection scoped to the Flask request context.
# #     Row factory is sqlite3.Row for dict-like rows.
# #     """
# #     db = getattr(g, "_database", None)
# #     if db is None:
# #         db = g._database = sqlite3.connect(DATABASE)
# #         db.row_factory = sqlite3.Row
# #     return db

# # # -------------------------
# # # LOANS TABLE INIT
# # # -------------------------
# # def init_loans_table():
# #     conn = sqlite3.connect(DATABASE)
# #     cur = conn.cursor()
# #     cur.execute("""
# #     CREATE TABLE IF NOT EXISTS loans (
# #         id INTEGER PRIMARY KEY AUTOINCREMENT,
# #         loanId TEXT UNIQUE,
# #         user_id TEXT,
# #         phone TEXT,                -- 🔹 NEW: borrower's phone number for payouts
# #         investment_id TEXT,        -- 🔹 links this loan to an investment
# #         amount REAL,
# #         interest REAL,
# #         status TEXT,               -- PENDING, APPROVED, DISAPPROVED, PAID
# #         expected_return_date TEXT,
# #         created_at TEXT,
# #         approved_by TEXT
# #     )
# #     """)

# #     conn.commit()
# #     conn.close()
    
# # with app.app_context():
# #     init_loans_table()

# # def migrate_loans_table():
# #     db = get_db()
# #     existing_columns = [col["name"] for col in db.execute("PRAGMA table_info(loans)").fetchall()]

# #     # ✅ Ensure disbursed_at column exists
# #     if "disbursed_at" not in existing_columns:
# #         db.execute("ALTER TABLE loans ADD COLUMN disbursed_at TEXT")
# #         db.commit()
# #         print("✅ Added missing column: disbursed_at")

# #     db.close()


# # # ✅ run migrations safely once app starts
# # with app.app_context():
# #     init_db()
# #     migrate_loans_table()


# # # -------------------------
# # # REQUEST A LOAN
# # # # -------------------------
# # import uuid
# # import sqlite3
# # from flask import Flask, jsonify, request
# # from datetime import datetime

# # app = Flask(__name__)

# # DB_PATH = "estack.db"

# # def get_db():
# #     db = sqlite3.connect(DB_PATH)
# #     db.row_factory = sqlite3.Row
# #     return db


# # # ------------------------
# # # 1️⃣ REQUEST A LOAN
# # # # ------------------------

# # # ------------------------
# # # 1️⃣ REQUEST A LOAN
# # # ------------------------
# # @app.route("/api/transactions/request", methods=["POST"])
# # def request_loan():
# #     try:
# #         data = request.get_json(force=True)
# #         phone = data.get("phone")
# #         amount = data.get("amount")
# #         investment_id = data.get("investment_id")

# #         print(f"📨 Received investment_id: {investment_id}")

# #         # ✅ Validate required fields
# #         if not phone or not investment_id or not amount:
# #             return jsonify({"error": "Missing required fields"}), 400

# #         db = get_db()
# #         cur = db.cursor()

# #         # ✅ Find investment that includes this ID and is COMPLETED
# #         cur.execute(
# #             """
# #             SELECT * FROM estack_transactions 
# #             WHERE name_of_transaction LIKE ? 
# #             AND status = 'COMPLETED'
# #             """,
# #             (f"%{investment_id}%",)
# #         )
# #         investment = cur.fetchone()

# #         if not investment:
# #             db.close()
# #             return jsonify({"error": "Investment not found or not completed"}), 404

# #         print(f"✅ Found matching investment: {investment['name_of_transaction']}")

# #         # ✅ Generate unique loan ID and name
# #         loan_id = str(uuid.uuid4())
# #         loan_name = f"LOAN | ZMW{amount} | {phone} | {investment_id} | {loan_id}"

# #         # ✅ Insert new loan record
# #         cur.execute(
# #             "INSERT INTO estack_transactions (name_of_transaction, status) VALUES (?, ?)",
# #             (loan_name, "ACTIVE")
# #         )

# #         # ✅ Mark investment as IN_USE
# #         cur.execute(
# #             "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
# #             ("IN_USE", f"%{investment_id}%")
# #         )

# #         db.commit()
# #         db.close()

# #         print(f"💰 Loan {loan_id} created for borrower {phone} using investment {investment_id}")

# #         return jsonify({
# #             "message": "Loan request recorded successfully",
# #             "loan_id": loan_id,
# #             "status": "ACTIVE"
# #         }), 200

# #     except Exception as e:
# #         print("❌ Error in /api/transactions/request:", e)
# #         return jsonify({"error": str(e)}), 500






# # # @app.route("/api/transactions/request", methods=["POST"])
# # # def request_loan():
# # #     try:
# # #         data = request.get_json(force=True)
# # #         phone = data.get("phone")
# # #         amount = data.get("amount")
# # #         # user_id = data.get("user_id")
# # #         investment_id = data.get("investment_id")

# # #         print(str(investment_id) + "from client")
# # #         # expected_return_date = data.get("expected_return_date", "")
# # #         # interest = data.get("interest", 0)

# # #         # if not phone or not user_id or not investment_id or not amount:
# # #         if not phone or not investment_id or not amount:
# # #             return jsonify({"error": "Missing required fields"}), 400

# # #         db = get_db()
# # #         cur = db.cursor()
# # # #_________________________________________________________________________________________________
# # #         # cur.execute("SELECT name_of_transaction FROM estack_transactions WHERE status = 'COMPLETED'")
# # #         cur.execute(
# # #                 "SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ? AND status = 'COMPLETED'",
# # #                 (f"%{investment_id}%",)
# # #             )
# # #         row = cur.fetchone()

# # #         if not row:
# # #             db.close()
# # #             return jsonify({"error": "No accepted transactions found"}), 404
        
# # #         name_of_transaction = row["name_of_transaction"]
        
# # #         # Extract investment ID (last element)
# # #         parts = [p.strip() for p in name_of_transaction.split("|")]
# # #         investment_id = parts[-1] if parts else None
        
# # #         if investment_id:
# # #             # ✅ Check if investment exists and is ACCEPTED
# # #             cur.execute(
# # #                 "SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ? AND status = 'COMPLETED'",
# # #                 (f"%{investment_id}%",)
# # #             )
# # #             # print(str(
# # # #_____________________________________________________________________________________________________________
# # #         # ✅ Check if investment exists and is ACCEPTED
# # #         # cur.execute("SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ?", (f"%{investment_id}%",))
# # #         investment = cur.fetchone()
# # #         # inv = cur.fetchone()
# # #         if investment:
# # #             print(f"✅ Found investment {investment_id}")
# # #         else:
# # #             print("❌ Investment not found or not accepted")
# # #         # else:
# # #         #     print("⚠️ Could not extract investment_id properly")
            
# # #         print(str(investment))
# # #         if not investment:
# # #             db.close()
# # #             print("Status1: " + str(investment["status"].upper()))# != "ACCEPTED"
# # #             return jsonify({"error": "Investment not found"}), 404

# # #         if investment["status"].upper() != "ACCEPTED":
# # #             db.close()
# # #             print("invest2: " + str(investment))
# # #             print("Status2: " + str(investment["status"].upper()))
# # #             return jsonify({"error": f"Investment not available (status={investment['status']})"}), 400

# # #         # ✅ Generate loan transaction
# # #         loan_id = str(uuid.uuid4())
# # #         loan_name = f"LOAN | ZMW{amount} | {investment_id} | {loan_id}"

# # #         # Insert loan transaction
# # #         cur.execute(
# # #             "INSERT INTO estack_transactions (name_of_transaction, status) VALUES (?, ?)",
# # #             (loan_name, "ACTIVE")
# # #         )

# # #         # Update linked investment to IN_USE
# # #         cur.execute(
# # #             "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
# # #             ("IN_USE", f"%{investment_id}%")
# # #         )

# # #         db.commit()
# # #         db.close()

# # #         print(f"💰 Loan {loan_id} created for user {user_id}, investment {investment_id}")

# # #         return jsonify({
# # #             "message": "Loan request recorded successfully",
# # #             "loanId": loan_id,
# # #             "status": "ACTIVE"
# # #         }), 200

# # #     except Exception as e:
# # #         print("❌ Error in /api/transactions/request:", e)
# # #         return jsonify({"error": str(e)}), 500


# # # # ------------------------
# # # # 2️⃣ GET USER LOANS
# # # # ------------------------
# # # @app.route("/api/loans/user/<user_id>", methods=["GET"])
# # # def get_user_loans(user_id):
# # #     try:
# # #         db = get_db()
# # #         cur = db.cursor()

# # #         cur.execute(
# # #             "SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ? AND name_of_transaction LIKE ?",
# # #             ("%LOAN%", f"%{user_id}%")
# # #         )
# # #         rows = cur.fetchall()
# # #         db.close()

# # #         results = []
# # #         for r in rows:
# # #             name = r["name_of_transaction"]
# # #             parts = [p.strip() for p in name.split("|")]
# # #             entry = {
# # #                 "name": name,
# # #                 "loanId": parts[-1] if len(parts) > 3 else "N/A",
# # #                 "amount": parts[1] if len(parts) > 1 else "N/A",
# # #                 "status": r["status"]
# # #             }
# # #             results.append(entry)

# # #         return jsonify(results), 200

# # #     except Exception as e:
# # #         print("❌ Error fetching loans:", e)
# # #         return jsonify({"error": str(e)}), 500

# # # ------------------------
# # # 2️⃣ GET USER LOANS
# # # ------------------------
# # @app.route("/api/loans/user/<user_id>", methods=["GET"])
# # def get_user_loans(user_id):
# #     try:
# #         db = get_db()
# #         cur = db.cursor()

# #         # Fetch transactions linked to this user (investor or borrower)
# #         cur.execute(
# #             """
# #             SELECT * FROM estack_transactions
# #             WHERE name_of_transaction LIKE ?
# #             ORDER BY rowid DESC
# #             """,
# #             (f"%{user_id}%",)
# #         )
# #         rows = cur.fetchall()
# #         db.close()

# #         results = []
# #         for r in rows:
# #             name = r["name_of_transaction"]
# #             parts = [p.strip() for p in name.split("|")]

# #             # Example:
# #             # INVESTMENT | K1000 | user_12 | 0f59ea4f-bc6d | Borrower:260977364437
# #             loan_id = parts[3] if len(parts) > 3 else "N/A"
# #             amount = parts[1].replace("K", "").strip() if len(parts) > 1 else "N/A"
# #             borrower = None

# #             # Check for borrower info
# #             if len(parts) > 4 and "Borrower:" in parts[4]:
# #                 borrower = parts[4].split(":", 1)[1]

# #             entry = {
# #                 "loan_id": loan_id,
# #                 "amount": amount,
# #                 "borrower": borrower or "N/A",
# #                 "status": r["status"],
# #             }
# #             results.append(entry)

# #         return jsonify(results), 200

# #     except Exception as e:
# #         print("❌ Error fetching loans:", e)
# #         return jsonify({"error": str(e)}), 500

# # @app.route("/api/transactions/request", methods=["POST"])
# # def create_loan_request():
# #     try:
# #         data = request.get_json()
# #         print("data: " + str(data))
# #         required = ["borrower_phone", "investment_id", "amount"]
# #         missing = [f for f in required if f not in data]

# #         if missing:
# #             return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

# #         borrower_phone = data["borrower_phone"]
# #         investment_id = data["investment_id"]
# #         amount = data["amount"]

# #         conn = get_db()
# #         cur = conn.cursor()

# #         # 🔍 1️⃣ Check if investment exists and is available
# #         cur.execute(
# #             "SELECT name_of_transaction, status FROM estack_transactions WHERE name_of_transaction LIKE ?",
# #             (f"%{investment_id}%",)
# #         )
# #         investment = cur.fetchone()

# #         if not investment:
# #             conn.close()
# #             return jsonify({"error": "Investment not found"}), 404

# #         if investment["status"] != "AVAILABLE":
# #             conn.close()
# #             return jsonify({"error": "Investment already loaned or pending"}), 400

# #         # 🧩 2️⃣ Update record to include borrower details
# #         old_name = investment["name_of_transaction"]
# #         # Example: "INVESTMENT | K1000 | user_12 | 0f59ea4f-bc6d"
# #         new_name = f"{old_name} | Borrower:{borrower_phone}"

# #         cur.execute(
# #             "UPDATE estack_transactions SET name_of_transaction = ?, status = ? WHERE name_of_transaction = ?",
# #             (new_name, "REQUESTED", old_name)
# #         )

# #         conn.commit()
# #         conn.close()

# #         print(f"✅ Loan requested: {new_name}")

# #         return jsonify({
# #             "message": "Loan request recorded successfully",
# #             "investment_id": investment_id,
# #             "status": "REQUESTED"
# #         }), 200

# #     except Exception as e:
# #         print("❌ Error in /api/transactions/request:", e)
# #         return jsonify({"error": str(e)}), 500



# # # ------------------------
# # # 3️⃣ MARK LOAN AS REPAID
# # # ------------------------
# # @app.route("/api/loans/repay/<loan_id>", methods=["POST"])
# # def repay_loan(loan_id):
# #     try:
# #         db = get_db()
# #         cur = db.cursor()

# #         # Find the loan transaction
# #         cur.execute(
# #             "SELECT name_of_transaction FROM estack_transactions WHERE name_of_transaction LIKE ?",
# #             (f"%{loan_id}%",)
# #         )
# #         loan = cur.fetchone()

# #         if not loan:
# #             db.close()
# #             return jsonify({"error": "Loan not found"}), 404

# #         # Mark the loan as REPAID
# #         cur.execute(
# #             "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
# #             ("REPAID", f"%{loan_id}%")
# #         )

# #         # Extract user_id from transaction
# #         parts = [p.strip() for p in loan["name_of_transaction"].split("|")]
# #         user_id = parts[2] if len(parts) > 2 else None

# #         # Make the user's investment AVAILABLE again
# #         if user_id:
# #             cur.execute(
# #                 """
# #                 UPDATE estack_transactions
# #                 SET status = ?
# #                 WHERE name_of_transaction LIKE ?
# #                 AND name_of_transaction NOT LIKE ?
# #                 """,
# #                 ("AVAILABLE", f"%{user_id}%", "%LOAN%")
# #             )

# #         db.commit()
# #         db.close()

# #         print(f"✅ Loan {loan_id} repaid — investment set to AVAILABLE")

# #         return jsonify({"message": "Loan repaid successfully"}), 200

# #     except Exception as e:
# #         print("❌ Error in repay_loan:", e)
# #         return jsonify({"error": str(e)}), 500

# # # # ------------------------
# # # # 3️⃣ MARK LOAN AS REPAID
# # # # ------------------------
# # # @app.route("/api/loans/repay/<loan_id>", methods=["POST"])
# # # def repay_loan(loan_id):
# # #     try:
# # #         db = get_db()
# # #         cur = db.cursor()

# # #         # ✅ Find the loan
# # #         cur.execute("SELECT name_of_transaction FROM estack_transactions WHERE name_of_transaction LIKE ?", (f"%{loan_id}%",))
# # #         loan = cur.fetchone()

# # #         if not loan:
# # #             db.close()
# # #             return jsonify({"error": "Loan not found"}), 404

# # #         # ✅ Mark loan as REPAID
# # #         cur.execute(
# # #             "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
# # #             ("REPAID", f"%{loan_id}%")
# # #         )

# # #         # ✅ Find linked investment ID (4th part of transaction)
# # #         parts = [p.strip() for p in loan["name_of_transaction"].split("|")]
# # #         user_id = parts[2] if len(parts) > 2 else None

# # #         # Mark investment AVAILABLE again
# # #         if user_id:
# # #             cur.execute(
# # #                 "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ? AND name_of_transaction NOT LIKE ?",
# # #                 ("AVAILABLE", f"%{user_id}%", "%LOAN%")
# # #             )

# # #         db.commit()
# # #         db.close()

# # #         print(f"✅ Loan {loan_id} repaid, investment reset to AVAILABLE")

# # #         return jsonify({"message": "Loan repaid successfully"}), 200

# # #     except Exception as e:
# # #         print("❌ Error in repay_loan:", e)
# # #         return jsonify({"error": str(e)}), 500

# # # @app.route("/api/loans/request", methods=["POST"])
# # # def request_loan():
# # #     data = request.json
# # #     user_id = data.get("user_id")
# # #     investment_id = data.get("investment_id")
# # #     amount = data.get("amount")
# # #     interest = data.get("interest", 5)
# # #     expected_return_date = data.get("expected_return_date")
# # #     phone = data.get("phone")  # <- NEW

# # #     if not user_id or not amount or not expected_return_date or not investment_id or not phone:
# # #         return jsonify({"error": "Missing required fields"}), 400

# # #     loanId = str(uuid.uuid4())
# # #     created_at = datetime.utcnow().isoformat()

# # #     conn = sqlite3.connect(DATABASE)
# # #     cur = conn.cursor()
# # #     cur.execute("""
# # #         INSERT INTO loans (loanId, user_id, investment_id, amount, interest, status, expected_return_date, created_at, phone)
# # #         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
# # #     """, (loanId, user_id, investment_id, amount, interest, "PENDING", expected_return_date, created_at, phone))
# # #     conn.commit()
# # #     conn.close()

# # #     return jsonify({"loanId": loanId, "status": "PENDING"}), 200

# # # -------------------------
# # # LIST PENDING LOANS (ADMIN VIEW)
# # # -------------------------
# # @app.route("/api/loans/pending", methods=["GET"])
# # def pending_loans():
# #     db = get_db()
# #     rows = db.execute("SELECT * FROM loans WHERE status='PENDING' ORDER BY created_at DESC").fetchall()
# #     results = [dict(row) for row in rows]
# #     return jsonify(results), 200


# # # -------------------------
# # # APPROVE LOAN
# # # # -------------------------
# # @app.route("/api/loans/approve/<loan_id>", methods=["POST"])
# # def approve_loan(loan_id):
# #     try:
# #         db = get_db()
# #         admin_id = request.json.get("admin_id", "admin_default")

# #         # ✅ Fetch loan by loanId
# #         loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
# #         if not loan:
# #             return jsonify({"error": "Loan not found"}), 404

# #         # ✅ Prevent double approval
# #         if loan["status"] and loan["status"].upper() == "APPROVED":
# #             return jsonify({"message": "Loan already approved"}), 200

# #         now = datetime.utcnow().isoformat()

# #         # ✅ Approve loan
# #         db.execute("""
# #             UPDATE loans
# #             SET status = 'APPROVED',
# #                 approved_by = ?,
# #                 approved_at = ?,
# #                 updated_at = ?
# #             WHERE loanId = ?
# #         """, (admin_id, now, now, loan_id))

# #         # ✅ Update investor’s transaction using investment_id, not user_id
# #         if loan["investment_id"]:
# #             db.execute("""
# #                 UPDATE transactions
# #                 SET status = 'LOANED_OUT',
# #                     updated_at = ?,
# #                     failureMessage = 'Loan Approved',
# #                     failureCode = 'LOAN'
# #                 WHERE depositId = ?
# #             """, (now, loan["investment_id"]))
# #             logger.info(f"✅ Investor transaction {loan['investment_id']} marked as LOANED_OUT.")

# #             # ✅ Notify investor
# #             txn = db.execute("SELECT user_id FROM transactions WHERE depositId=?", (loan["investment_id"],)).fetchone()
# #             if txn and txn["user_id"]:
# #                 notify_investor(txn["user_id"], f"Your investment {loan['investment_id']} has been loaned out.")

# #         db.commit()
# #         return jsonify({"message": f"Loan {loan_id} approved and linked investor updated"}), 200

# #     except Exception as e:
# #         db.rollback()
# #         logger.exception("Error approving loan")
# #         return jsonify({"error": str(e)}), 500

# # # @app.route("/api/loans/approve/<loan_id>", methods=["POST"])
# # # def approve_loan(loan_id):
# # #     try:
# # #         db = get_db()
# # #         admin_id = request.json.get("admin_id", "admin_default")

# # #         # Check if loan exists
# # #         loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
# # #         if not loan:
# # #             return jsonify({"error": "Loan not found"}), 404

# # #         # Prevent double approval
# # #         if loan["status"].upper() == "APPROVED":
# # #             return jsonify({"message": "Loan already approved"}), 200

# # #         # Update loan status
# # #         db.execute("""
# # #             UPDATE loans
# # #             SET status = 'APPROVED',
# # #                 approved_by = ?,
# # #                 approved_at = ?,
# # #                 updated_at = ?
# # #             WHERE loanId = ?
# # #         """, (
# # #             admin_id,
# # #             datetime.utcnow().isoformat(),
# # #             datetime.utcnow().isoformat(),
# # #             loan_id
# # #         ))
# # #         db.commit()

# # #         # Update investor transaction if exists
# # #         db.execute("""
# # #             UPDATE transactions
# # #             SET status = 'LOANED_OUT',
# # #                 updated_at = ?,
# # #                 metadata = COALESCE(metadata, ''),
# # #                 failureMessage = 'Loan Approved',
# # #                 failureCode = 'LOAN'
# # #             WHERE user_id = ? AND type = 'investment'
# # #         """, (datetime.utcnow().isoformat(), loan["user_id"]))
# # #         db.commit()

# # #         return jsonify({"message": f"Loan {loan_id} approved successfully"}), 200

# # #     except Exception as e:
# # #         db.rollback()
# # #         return jsonify({"error": str(e)}), 500

# # # -------------------------
# # # DISAPPROVE LOAN
# # # -------------------------
# # @app.route("/api/loans/disapprove/<loan_id>", methods=["POST"])
# # def disapprove_loan(loan_id):
# #     db = get_db()
# #     db.execute("UPDATE loans SET status='DISAPPROVED' WHERE loanId=?", (loan_id,))
# #     db.commit()
# #     return jsonify({"message": "Loan disapproved"}), 200


# # # -------------------------
# # # INVESTOR LOANS VIEW
# # # -------------------------
# # @app.route("/api/loans/user/<user_id>", methods=["GET"])
# # def user_loans(user_id):
# #     db = get_db()
# #     rows = db.execute("SELECT * FROM loans WHERE user_id=? ORDER BY created_at DESC", (user_id,)).fetchall()
# #     results = [dict(row) for row in rows]
# #     return jsonify(results), 200

# # @app.teardown_appcontext
# # def close_connection(exception):
# #     db = getattr(g, "_database", None)
# #     if db is not None:
# #         db.close()


# # # -------------------------
# # # HEALTH
# # # -------------------------
# # @app.route("/")
# # def home():
# #     return f"PawaPay Callback Receiver running ✅ (API_MODE={API_MODE})"


# # # -------------------------
# # # ORIGINAL PAYMENT ENDPOINTS
# # # -------------------------
# # @app.route("/initiate-payment", methods=["POST"])
# # def initiate_payment():
# #     try:
# #         data = request.json
# #         phone = data.get("phone")
# #         amount = data.get("amount")
# #         if not phone or not amount:
# #             return jsonify({"error": "Missing phone or amount"}), 400

# #         deposit_id = str(uuid.uuid4())
# #         customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# #         payload = {
# #             "depositId": deposit_id,
# #             "amount": str(amount),
# #             "currency": "ZMW",
# #             "correspondent": "MTN_MOMO_ZMB",
# #             "payer": {"type": "MSISDN", "address": {"value": phone}},
# #             "customerTimestamp": customer_ts,
# #             "statementDescription": "StudyCraftPay",
# #             "metadata": [
# #                 {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
# #                 {"fieldName": "customerId", "fieldValue": phone, "isPII": True},
# #             ],
# #         }

# #         headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
# #         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)
# #         result = {}
# #         try:
# #             result = resp.json()
# #         except Exception:
# #             logger.warning("Non-JSON response from PawaPay for initiate-payment: %s", resp.text)

# #         db = get_db()
# #         db.execute("""
# #             INSERT OR REPLACE INTO transactions
# #             (depositId,status,amount,currency,phoneNumber,provider,
# #              providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
# #             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
# #         """, (
# #             deposit_id,
# #             result.get("status", "PENDING"),
# #             float(amount),
# #             "ZMW",
# #             phone,
# #             None, None, None, None,
# #             json.dumps(payload["metadata"]),
# #             datetime.utcnow().isoformat(),
# #             "payment",
# #             None
# #         ))
# #         db.commit()
# #         logger.info("initiate-payment: inserted depositId=%s status=%s", deposit_id, result.get("status", "PENDING"))
# #         return jsonify({"depositId": deposit_id, **result}), 200

# #     except Exception:
# #         logger.exception("Payment initiation error")
# #         return jsonify({"error": "Internal server error"}), 500

# # # -------------------------
# # # CALLBACK RECEIVER (upsert-safe for deposits and payouts)
# # # -------------------------

# # #Test 2 callback 2
# # @app.route("/callback/deposit", methods=["POST"])
# # def deposit_callback():
# #     try:
# #         data = request.get_json(force=True)
# #         print("📩 Full callback data:", data)

# #         # Identify app type: StudyCraft vs eStack
# #         metadata = data.get("metadata", {})
# #         is_estack = isinstance(metadata, dict) and "userId" in metadata
# #         is_studycraft = "payer" in data or "recipient" in data

# #         # =====================================================
# #         # 🔹 Case 1: eStack Application
# #         # =====================================================
# #         if is_estack:
# #             deposit_id = data.get("depositId")
# #             status = data.get("status", "PENDING").strip().upper()
# #             amount = data.get("depositedAmount", 0)
# #             user_id = metadata.get("userId", "unknown")

# #             if not deposit_id:
# #                 return jsonify({"error": "Missing depositId"}), 400

# #             name_of_transaction = f"ZMW{amount} | {user_id} | {deposit_id}"

# #             db = sqlite3.connect("estack.db")
# #             db.row_factory = sqlite3.Row
# #             cur = db.cursor()

# #             cur.execute("""
# #                 CREATE TABLE IF NOT EXISTS estack_transactions (
# #                     name_of_transaction TEXT NOT NULL,
# #                     status TEXT NOT NULL
# #                 )
# #             """)

# #             existing = cur.execute(
# #                 "SELECT name_of_transaction FROM estack_transactions WHERE name_of_transaction LIKE ?",
# #                 (f"%{deposit_id}%",)
# #             ).fetchone()

# #             if existing:
# #                 cur.execute(
# #                     "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
# #                     (status, f"%{deposit_id}%")
# #                 )
# #                 print(f"🔄 Updated eStack transaction {deposit_id} → {status}")
# #             else:
# #                 cur.execute(
# #                     "INSERT INTO estack_transactions (name_of_transaction, status) VALUES (?, ?)",
# #                     (name_of_transaction, status)
# #                 )
# #                 print(f"💾 Inserted new eStack transaction {deposit_id} → {status}")

# #             db.commit()
# #             db.close()

# #             # ✅ Dropbox Sync (optional if using persistence)
# #             try:
# #                 from database_backup import upload_db
# #                 upload_db()
# #             except Exception as sync_err:
# #                 print("⚠️ Dropbox sync skipped:", sync_err)

# #             return jsonify({"success": True, "source": "eStack", "deposit_id": deposit_id, "status": status}), 200

# #         # =====================================================
# #         # 🔹 Case 2: StudyCraft Application
# #         # =====================================================
# #         elif is_studycraft:
# #             deposit_id = data.get("depositId")
# #             payout_id = data.get("payoutId")

# #             if not deposit_id and not payout_id:
# #                 return jsonify({"error": "Missing depositId/payoutId"}), 400

# #             txn_type = "payment" if deposit_id else "payout"
# #             txn_id = deposit_id or payout_id
# #             status = data.get("status")
# #             amount = data.get("amount")
# #             currency = data.get("currency")

# #             if txn_type == "payment":
# #                 phone = data.get("payer", {}).get("accountDetails", {}).get("phoneNumber")
# #                 provider = data.get("payer", {}).get("accountDetails", {}).get("provider")
# #             else:
# #                 phone = data.get("recipient", {}).get("accountDetails", {}).get("phoneNumber")
# #                 provider = data.get("recipient", {}).get("accountDetails", {}).get("provider")

# #             provider_txn = data.get("providerTransactionId")
# #             failure_code = data.get("failureReason", {}).get("failureCode")
# #             failure_message = data.get("failureReason", {}).get("failureMessage")

# #             user_id, loan_id = None, None
# #             metadata_obj = metadata
# #             if metadata_obj:
# #                 if isinstance(metadata_obj, dict):
# #                     user_id = metadata_obj.get("userId")
# #                     loan_id = metadata_obj.get("loanId")
# #                 elif isinstance(metadata_obj, list):
# #                     for entry in metadata_obj:
# #                         if isinstance(entry, dict):
# #                             if entry.get("fieldName") == "userId":
# #                                 user_id = entry.get("fieldValue")
# #                             if entry.get("fieldName") == "loanId":
# #                                 loan_id = entry.get("fieldValue")

# #             db = get_db()
# #             existing = db.execute(
# #                 "SELECT * FROM transactions WHERE depositId=? OR depositId=?",
# #                 (deposit_id, payout_id)
# #             ).fetchone()

# #             now_iso = datetime.utcnow().isoformat()
# #             metadata_str = json.dumps(metadata_obj) if metadata_obj else None

# #             if existing:
# #                 db.execute("""
# #                     UPDATE transactions
# #                     SET status = COALESCE(?, status),
# #                         amount = COALESCE(?, amount),
# #                         currency = COALESCE(?, currency),
# #                         phoneNumber = COALESCE(?, phoneNumber),
# #                         provider = COALESCE(?, provider),
# #                         providerTransactionId = COALESCE(?, providerTransactionId),
# #                         failureCode = COALESCE(?, failureCode),
# #                         failureMessage = COALESCE(?, failureMessage),
# #                         metadata = COALESCE(?, metadata),
# #                         updated_at = ?,
# #                         user_id = COALESCE(?, user_id)
# #                     WHERE depositId = ? OR depositId = ?
# #                 """, (
# #                     status,
# #                     float(amount) if amount else None,
# #                     currency,
# #                     phone,
# #                     provider,
# #                     provider_txn,
# #                     failure_code,
# #                     failure_message,
# #                     metadata_str,
# #                     now_iso,
# #                     user_id,
# #                     deposit_id,
# #                     payout_id
# #                 ))
# #             else:
# #                 db.execute("""
# #                     INSERT INTO transactions
# #                     (depositId, status, amount, currency, phoneNumber, provider, providerTransactionId,
# #                      failureCode, failureMessage, metadata, received_at, updated_at, type, user_id)
# #                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
# #                 """, (
# #                     txn_id,
# #                     status,
# #                     float(amount) if amount else None,
# #                     currency,
# #                     phone,
# #                     provider,
# #                     provider_txn,
# #                     failure_code,
# #                     failure_message,
# #                     metadata_str,
# #                     now_iso,
# #                     now_iso,
# #                     txn_type,
# #                     user_id
# #                 ))

# #             # ✅ Handle loan repayment notification
# #             if txn_type == "payout" and loan_id and status in ("COMPLETED", "SUCCESS", "PAYMENT_COMPLETED"):
# #                 db.execute("UPDATE loans SET status=? WHERE loanId=?", (status, loan_id))
# #                 loan_row = db.execute("SELECT user_id FROM loans WHERE loanId=?", (loan_id,)).fetchone()
# #                 if loan_row and loan_row["user_id"]:
# #                     notify_investor(
# #                         loan_row["user_id"],
# #                         f"Loan {loan_id[:8]} has been successfully repaid."
# #                     )

# #             db.commit()
# #             return jsonify({"received": True, "source": "StudyCraft"}), 200

# #         # =====================================================
# #         # 🔹 Unknown callback structure
# #         # =====================================================
# #         else:
# #             return jsonify({"error": "Unknown callback format"}), 400

# #     except Exception as e:
# #         print("❌ Unified callback error:", e)
# #         return jsonify({"error": str(e)}), 500

# # # -------------------------
# # # DEPOSIT STATUS / TRANSACTION LOOKUP
# # # -------------------------
# # # @app.route("/deposit_status/<deposit_id>")
# # # def deposit_status(deposit_id):
# # #     db = get_db()
# # #     row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
# # #     if not row:
# # #         return jsonify({"status": None, "message": "Deposit not found"}), 404
# # #     res = {k: row[k] for k in row.keys()}
# # #     if res.get("metadata"):
# # #         try:
# # #             res["metadata"] = json.loads(res["metadata"])
# # #         except:
# # #             pass
# # #     return jsonify(res), 200

# # # @app.route("/transactions/<deposit_id>")
# # # def get_transaction(deposit_id):
# # #     db = get_db()
# # #     row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
# # #     if not row:
# # #         return jsonify({"error": "not found"}), 404
# # #     res = {k: row[k] for k in row.keys()}
# # #     if res.get("metadata"):
# # #         try:
# # #             res["metadata"] = json.loads(res["metadata"])
# # #         except:
# # #             pass
# # #     return jsonify(res), 200

# # # -------------------------
# # # INVESTMENT ENDPOINTS
# # # -------------------------
# # # @app.route("/api/investments/initiate", methods=["POST"])
# # # def initiate_investment():
# # #     try:
# # #         data = request.json or {}
# # #         # Support both "phone" and "phoneNumber" keys from different clients
# # #         phone = data.get("phone") or data.get("phoneNumber")
# # #         amount = data.get("amount")
# # #         correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
# # #         currency = data.get("currency", "ZMW")
# # #         # prefer explicit user_id, but don't crash if missing
# # #         user_id = data.get("user_id") or data.get("userId") or "unknown"

# # #         if not phone or amount is None:
# # #             return jsonify({"error": "Missing phone or amount"}), 400

# # #         deposit_id = str(uuid.uuid4())
# # #         customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# # #         payload = {
# # #             "depositId": deposit_id,
# # #             "amount": str(amount),
# # #             "currency": currency,
# # #             "correspondent": correspondent,
# # #             "payer": {"type": "MSISDN", "address": {"value": phone}},
# # #             "customerTimestamp": customer_ts,
# # #             "statementDescription": "Investment",
# # #             "metadata": [
# # #                 {"fieldName": "purpose", "fieldValue": "investment"},
# # #                 {"fieldName": "userId", "fieldValue": str(user_id), "isPII": True},
# # #             ],
# # #         }

# # #         headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
# # #         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)

# # #         try:
# # #             result = resp.json()
# # #         except Exception:
# # #             logger.error("PawaPay response not JSON: %s", resp.text)
# # #             return jsonify({"error": "Invalid response from PawaPay"}), 502

# # #         status = result.get("status", "PENDING")

# # #         db = get_db()
# # #         # Insert a new investment record (depositId will be unique)
# # #         db.execute("""
# # #             INSERT OR REPLACE INTO transactions
# # #             (depositId,status,amount,currency,phoneNumber,provider,
# # #              providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
# # #             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
# # #         """, (
# # #             deposit_id,
# # #             status,
# # #             float(amount),
# # #             currency,
# # #             phone,
# # #             None, None, None, None,
# # #             json.dumps(payload["metadata"]),
# # #             datetime.utcnow().isoformat(),
# # #             "investment",
# # #             user_id
# # #         ))
# # #         db.commit()
# # #         logger.info("initiate_investment: inserted depositId=%s user_id=%s amount=%s status=%s",
# # #                     deposit_id, user_id, amount, status)

# # #         return jsonify({"depositId": deposit_id, "status": status}), 200

# # #     except Exception as e:
# # #         logger.exception("Investment initiation error")
# # #         return jsonify({"error": str(e)}), 500


# # # @app.route("/api/investments/user/<user_id>", methods=["GET"])
# # # def get_user_investments(user_id):
# # #     """
# # #     Return investments for a user. We select type='investment' and the exact user_id column.
# # #     This returns a list of rows (may be empty).
# # #     """
# # #     db = get_db()
# # #     rows = db.execute(
# # #         "SELECT * FROM transactions WHERE type='investment' AND user_id=? ORDER BY received_at DESC",
# # #         (user_id,)
# # #     ).fetchall()

# # #     results = []
# # #     for row in rows:
# # #         res = {k: row[k] for k in row.keys()}
# # #         if res.get("metadata"):
# # #             try:
# # #                 res["metadata"] = json.loads(res["metadata"])
# # #             except:
# # #                 pass
# # #         results.append(res)

# # #     return jsonify(results), 200


# # # # -------------------------
# # # # SAMPLE INVESTMENT ROUTE (handy for testing)
# # # # -------------------------
# # # @app.route("/sample-investment", methods=["POST"])
# # # def add_sample():
# # #     """Add a test investment to verify DB works"""
# # #     try:
# # #         db = get_db()
# # #         deposit_id = str(uuid.uuid4())
# # #         payload_metadata = [{"fieldName": "purpose", "fieldValue": "investment"},
# # #                             {"fieldName": "userId", "fieldValue": "user_1"}]
# # #         received_at = datetime.utcnow().isoformat()
# # #         db.execute("""
# # #             INSERT INTO transactions
# # #             (depositId,status,amount,currency,phoneNumber,metadata,received_at,type,user_id)
# # #             VALUES (?,?,?,?,?,?,?,?,?)
# # #         """, (
# # #             deposit_id,
# # #             "SUCCESS",
# # #             1000.0,
# # #             "ZMW",
# # #             "0965123456",
# # #             json.dumps(payload_metadata),
# # #             received_at,
# # #             "investment",
# # #             "user_1"
# # #         ))
# # #         db.commit()
# # #         logger.info("Added sample investment depositId=%s", deposit_id)
# # #         return jsonify({"message":"Sample investment added","depositId":deposit_id}), 200
# # #     except Exception as e:
# # #         logger.exception("Failed to insert sample")
# # #         return jsonify({"error": str(e)}), 500


# # # -------------------------
# # # OPTIONAL: debug route to see all transactions (helpful during testing)
# # # -------------------------
# # @app.route("/debug/transactions", methods=["GET"])
# # def debug_transactions():
# #     db = get_db()
# #     rows = db.execute("SELECT * FROM transactions ORDER BY received_at DESC").fetchall()
# #     results = []
# #     for row in rows:
# #         res = {k: row[k] for k in row.keys()}
# #         if res.get("metadata"):
# #             try:
# #                 res["metadata"] = json.loads(res["metadata"])
# #             except:
# #                 pass
# #         results.append(res)
# #     return jsonify(results), 200
    
# # #-----------------------------------
# # # GET PENDING REQUESTS
# # #----------------------------------

# # # @app.route("/api/loans/pending", methods=["GET"])
# # # def get_pending_loans():
# # #     conn = sqlite3.connect(DATABASE)
# # #     cur = conn.cursor()
# # #     cur.execute("SELECT loanId, user_id, amount, interest, status, expected_return_date FROM loans WHERE status = ?", ("PENDING",))
# # #     rows = cur.fetchall()
# # #     conn.close()

# # #     loans = []
# # #     for row in rows:
# # #         loans.append({
# # #             "loanId": row[0],
# # #             "user_id": row[1],
# # #             "amount": row[2],
# # #             "interest": row[3],
# # #             "status": row[4],
# # #             "expected_return_date": row[5]
# # #         })

# # #     return jsonify(loans), 200

# # # -------------------------
# # # DISBURSE LOAN (ADMIN ACTION)
# # # -------------------------

# # #TEST 4
# # @app.route("/api/loans/disburse/<loan_id>", methods=["POST"])
# # def disburse_loan(loan_id):
# #     try:
# #         db = get_db()  # ✅ Get the database connection
# #         data = request.get_json() or {}
# #         logger.info(f"Disbursing loan {loan_id} with data: {data}")

# #         # ✅ Fetch loan details (fixed column name)
# #         loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
# #         if not loan:
# #             return jsonify({"error": "Loan not found"}), 404

# #         # ✅ Ensure loan is approved before disbursement
# #         # if loan["status"] != "approved":
# #         #     return jsonify({"error": "Loan is not approved for disbursement"}), 400

# #         borrower_id = loan["user_id"]
# #         amount = float(loan["amount"])

# #         # ✅ Fetch borrower wallet
# #         borrower_wallet = db.execute(
# #             "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
# #         ).fetchone()
        
# #         if not borrower_wallet:
# #             db.execute("""
# #                 INSERT INTO wallets (user_id, balance, created_at, updated_at)
# #                 VALUES (?, 0, ?, ?)
# #             """, (borrower_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat()))
# #             db.commit()
# #             logger.info(f"✅ Created new wallet for borrower {borrower_id}")
        
# #             borrower_wallet = db.execute(
# #                 "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
# #             ).fetchone()

# #         borrower_balance = float(borrower_wallet["balance"])

# #         # ✅ Credit borrower wallet
# #         new_balance = borrower_balance + amount
# #         db.execute(
# #             "UPDATE wallets SET balance = ?, updated_at = ? WHERE user_id = ?",
# #             (new_balance, datetime.utcnow().isoformat(), borrower_id)
# #         )

# #         # ✅ Mark loan as disbursed (fixed column name)
# #         db.execute(
# #             "UPDATE loans SET status = 'disbursed', disbursed_at = ? WHERE loanId = ?",
# #             (datetime.utcnow().isoformat(), loan_id)
# #         )

# #         # ✅ Record the disbursement transaction
# #         db.execute("""
# #             INSERT INTO transactions (user_id, amount, type, status, reference, created_at, updated_at)
# #             VALUES (?, ?, 'loan_disbursement', 'SUCCESS', ?, ?, ?)
# #         """, (
# #             borrower_id, amount, loan_id,
# #             datetime.utcnow().isoformat(),
# #             datetime.utcnow().isoformat()
# #         ))

# #         db.commit()

# #         # ✅ Link this loan to one available investment
# #         try:
# #             # investment_row = db.execute("""
# #             #     SELECT depositId FROM transactions
# #             #     WHERE type = 'investment' AND status = 'ACTIVE'
# #             #     ORDER BY received_at ASC LIMIT 1
# #             # """).fetchone()
# #             investment_row = db.execute("""
# #                 SELECT reference FROM transactions
# #                 WHERE type = 'investment' AND status = 'ACTIVE'
# #                 ORDER BY created_at ASC LIMIT 1
# #             """).fetchone()


# #             if investment_row:
# #                 # investment_id = investment_row["depositId"]
# #                 investment_id = investment_row["reference"]


# #                 # ✅ Mark that single investment as LOANED_OUT
# #                 db.execute("""
# #                     UPDATE transactions
# #                     SET status = 'LOANED_OUT', updated_at = ?
# #                     WHERE reference = ?

# #                     # UPDATE transactions
# #                     # SET status = 'LOANED_OUT', investment_id = ?, updated_at = ?
# #                     # WHERE depositId = ?
# #                 """, (loan_id, datetime.utcnow().isoformat(), investment_id))
# #                 db.commit()

# #                 # ✅ Notify the investor
# #                 investor_row = db.execute("""
# #                     SELECT user_id FROM transactions
# #                     WHERE reference = ? AND type = 'investment'
# #                     # SELECT user_id FROM transactions
# #                     # WHERE depositId = ? AND type = 'investment'
# #                 """, (investment_id,)).fetchone()
# #                 # investment_row = db.execute("""
# #                 #     SELECT reference FROM transactions
# #                 #     WHERE type = 'investment' AND status = 'ACTIVE'
# #                 #     ORDER BY created_at ASC LIMIT 1
# #                 # """).fetchone()

# #                 # ✅ Also mark investor's transaction as DISBURSED
# #                 if loan["investment_id"]:
# #                     db.execute("""
# #                         UPDATE transactions
# #                         SET status = 'DISBURSED', updated_at = ?
# #                         WHERE depositId = ?
# #                     """, (datetime.utcnow().isoformat(), loan["investment_id"]))
# #                     logger.info(f"✅ Investor transaction {loan['investment_id']} marked as DISBURSED.")



# #                 if investor_row and investor_row["user_id"]:
# #                     notify_investor(
# #                         investor_row["user_id"],
# #                         f"Your investment {investment_id[:8]} has been loaned out to borrower {loan_id[:8]}."
# #                     )

# #                 logger.info(f"Investment {investment_id} linked to loan {loan_id}")
# #             else:
# #                 logger.warning("No available active investment found to link with this loan.")

# #         except Exception as e:
# #             logger.error(f"Error linking investment to loan {loan_id}: {e}")

# #         return jsonify({
# #             "message": f"Loan {loan_id} successfully disbursed",
# #             "borrower_id": borrower_id,
# #             "amount": amount,
# #             "new_balance": new_balance
# #         }), 200

# #     except Exception as e:
# #         logger.error(f"Error disbursing loan {loan_id}: {e}")
# #         return jsonify({"error": str(e)}), 500

# # # -------------------------
# # # REJECT LOAN (ADMIN ACTION)
# # # -------------------------
# # @app.route("/api/loans/reject/<loan_id>", methods=["POST"])
# # def reject_loan(loan_id):
# #     admin_id = request.json.get("admin_id", "admin_default")
# #     db = get_db()
# #     loan = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
# #     if not loan:
# #         return jsonify({"error": "Loan not found"}), 404
# #     if loan["status"] != "PENDING":
# #         return jsonify({"error": f"Loan already {loan['status']}"}), 400

# #     db.execute("UPDATE loans SET status='REJECTED', approved_by=? WHERE loanId=?", (admin_id, loan_id))
# #     db.commit()

# #     return jsonify({"loanId": loan_id, "status": "REJECTED"}), 200

# # # OPTIONAL CODE CHECK NOTIFICATION 
# # @app.route("/api/notifications/<user_id>", methods=["GET"])
# # def get_notifications(user_id):
# #     conn = sqlite3.connect(DATABASE)
# #     conn.row_factory = sqlite3.Row
# #     cur = conn.cursor()
# #     cur.execute("SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC", (user_id,))
# #     rows = cur.fetchall()
# #     conn.close()
# #     return jsonify([dict(row) for row in rows]), 200

# # # -------------------------
# # # RUN
# # # -------------------------

# # if __name__ == "__main__":
# #     with app.app_context():
# #         init_db()
# #     port = int(os.environ.get("PORT", 5000))
# #     app.run(host="0.0.0.0", port=port)


# # # from dotenv import load_dotenv
# # # load_dotenv()

# # # from flask import Flask, request, jsonify, g
# # # import os, logging, sqlite3, json, requests, uuid
# # # from datetime import datetime

# # # # -------------------------
# # # # API CONFIGURATION
# # # # -------------------------
# # # API_MODE = os.getenv("API_MODE", "sandbox")
# # # SANDBOX_API_TOKEN = os.getenv("SANDBOX_API_TOKEN")
# # # LIVE_API_TOKEN = os.getenv("LIVE_API_TOKEN")

# # # API_TOKEN = LIVE_API_TOKEN if API_MODE == "live" else SANDBOX_API_TOKEN
# # # PAWAPAY_URL = (
# # #     "https://api.pawapay.io/deposits"
# # #     if API_MODE == "live"
# # #     else "https://api.sandbox.pawapay.io/deposits"
# # # )

# # # PAWAPAY_PAYOUT_URL = (
# # #     "https://api.pawapay.io/v2/payouts"
# # #     if API_MODE == "live"
# # #     else "https://api.sandbox.pawapay.io/v2/payouts"
# # # )

# # # # -------------------------
# # # # DATABASE
# # # # -------------------------
# # # DATABASE_2 = os.path.join(os.path.dirname(__file__), "estack.db")
# # # app = Flask(__name__)
# # # logging.basicConfig(level=logging.INFO)
# # # logger = logging.getLogger(__name__)

# # # =========================
# # # ✅ DATABASE CONFIG
# # # =========================
# # DATABASE = os.path.join(os.path.dirname(__file__), "estack.db")

# # def init_db():
# #     """
# #     Create the estack_transactions table if missing.
# #     Stores combined transaction info and status only.
# #     """
# #     conn = sqlite3.connect(DATABASE)
# #     cur = conn.cursor()

# #     # ✅ Create the single table
# #     cur.execute("""
# #         CREATE TABLE IF NOT EXISTS estack_transactions (
# #             id INTEGER PRIMARY KEY AUTOINCREMENT,
# #             name_of_transaction TEXT NOT NULL,  -- e.g. "K1000 | user_123 | DEP4567"
# #             status TEXT NOT NULL,               -- e.g. "invested", "loaned_out", "repaid"
# #             created_at TEXT DEFAULT CURRENT_TIMESTAMP
# #         )
# #     """)

# #     conn.commit()
# #     conn.close()
# #     print("✅ estack.db initialized with estack_transactions table.")


# # # ✅ Initialize database once Flask app starts
# # with app.app_context():
# #     init_db()


# # def get_db():
# #     """
# #     Return a DB connection scoped to the Flask request context.
# #     Row factory is sqlite3.Row for dict-like access.
# #     """
# #     db = getattr(g, "_database", None)
# #     if db is None:
# #         db = g._database = sqlite3.connect(DATABASE)
# #         db.row_factory = sqlite3.Row
# #     return db
    
# # # # -------------------------
# # # # REQUEST A LOAN
# # # @app.route("/api/transactions/request", methods=["POST"])
# # # def request_loan():
# # #     try:
# # #         data = request.json or {}
# # #         borrower_id = data.get("borrower_id")
# # #         phone = data.get("phone")
# # #         amount = data.get("amount")
# # #         investment_id = data.get("investment_id")
# # #         interest = data.get("interest", 5)
# # #         expected_return_date = data.get("expected_return_date", "")

# # #         if not borrower_id or not amount:
# # #             return jsonify({"error": "Missing borrower_id or amount"}), 400

# # #         # Combine into readable name for same table format
# # #         # Example: "ZMW500 | user_1 | loan_abc123"
# # #         loan_id = str(uuid.uuid4())
# # #         name = f"ZMW{amount} | {borrower_id} | {loan_id}"
# # #         status = "REQUESTED"

# # #         db = get_db_2()
# # #         db.execute(
# # #             "INSERT INTO transactions (name, status) VALUES (?, ?)",
# # #             (name, status)
# # #         )
# # #         db.commit()

# # #         print(f"💸 Loan request recorded for {borrower_id}: {amount} ({loan_id})")

# # #         return jsonify({
# # #             "message": "Loan requested successfully",
# # #             "loan_id": loan_id,
# # #             "amount": amount,
# # #             "status": status
# # #         }), 200

# # #     except Exception as e:
# # #         logger.exception("Error requesting loan")
# # #         return jsonify({"error": str(e)}), 500

# # # # -------------------------
# # # # LIST PENDING LOANS (ADMIN VIEW)
# # # # -------------------------
# # # @app.route("/api/loans/pending", methods=["GET"])
# # # def pending_loans():
# # #     db = get_db_2()
# # #     rows = db.execute("SELECT * FROM loans WHERE status='PENDING' ORDER BY created_at DESC").fetchall()
# # #     results = [dict(row) for row in rows]
# # #     return jsonify(results), 200

# # # # -------------------------
# # # # APPROVE LOAN
# # # # # -------------------------
# # # @app.route("/api/loans/approve/<loan_id>", methods=["POST"])
# # # def approve_loan(loan_id):
# # #     try:
# # #         db = get_db_2()
# # #         admin_id = request.json.get("admin_id", "admin_default")

# # #         # ✅ Fetch loan by loanId
# # #         loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
# # #         if not loan:
# # #             return jsonify({"error": "Loan not found"}), 404

# # #         # ✅ Prevent double approval
# # #         if loan["status"] and loan["status"].upper() == "APPROVED":
# # #             return jsonify({"message": "Loan already approved"}), 200

# # #         now = datetime.utcnow().isoformat()

# # #         # ✅ Approve loan
# # #         db.execute("""
# # #             UPDATE loans
# # #             SET status = 'APPROVED',
# # #                 approved_by = ?,
# # #                 approved_at = ?,
# # #                 updated_at = ?
# # #             WHERE loanId = ?
# # #         """, (admin_id, now, now, loan_id))

# # #         # ✅ Update investor’s transaction using investment_id, not user_id
# # #         if loan["investment_id"]:
# # #             db.execute("""
# # #                 UPDATE transactions
# # #                 SET status = 'LOANED_OUT',
# # #                     updated_at = ?,
# # #                     failureMessage = 'Loan Approved',
# # #                     failureCode = 'LOAN'
# # #                 WHERE depositId = ?
# # #             """, (now, loan["investment_id"]))
# # #             logger.info(f"✅ Investor transaction {loan['investment_id']} marked as LOANED_OUT.")

# # #             # ✅ Notify investor
# # #             txn = db.execute("SELECT user_id FROM transactions WHERE depositId=?", (loan["investment_id"],)).fetchone()
# # #             if txn and txn["user_id"]:
# # #                 notify_investor(txn["user_id"], f"Your investment {loan['investment_id']} has been loaned out.")

# # #         db.commit()
# # #         return jsonify({"message": f"Loan {loan_id} approved and linked investor updated"}), 200

# # #     except Exception as e:
# # #         db.rollback()
# # #         logger.exception("Error approving loan")
# # #         return jsonify({"error": str(e)}), 500


# # # # -------------------------
# # # # DISAPPROVE LOAN
# # # # -------------------------
# # # @app.route("/api/loans/disapprove/<loan_id>", methods=["POST"])
# # # def disapprove_loan(loan_id):
# # #     db = get_db_2()
# # #     db.execute("UPDATE loans SET status='DISAPPROVED' WHERE loanId=?", (loan_id,))
# # #     db.commit()
# # #     return jsonify({"message": "Loan disapproved"}), 200

# # # # -------------------------
# # # # INVESTOR LOANS VIEW
# # # # -------------------------
# # # @app.route("/api/loans/user/<user_id>", methods=["GET"])
# # # def user_loans(user_id):
# # #     db = get_db_2()
# # #     rows = db.execute("SELECT * FROM loans WHERE user_id=? ORDER BY created_at DESC", (user_id,)).fetchall()
# # #     results = [dict(row) for row in rows]
# # #     return jsonify(results), 200

# # # @app.teardown_appcontext
# # # def close_connection(exception):
# # #     db = getattr(g, "_database", None)
# # #     if db is not None:
# # #         db.close()

# # # # -------------------------
# # # # HEALTH
# # # # -------------------------
# # # @app.route("/")
# # # def home():
# # #     return f"PawaPay Callback Receiver running ✅ (API_MODE={API_MODE})"

# # # # -------------------------
# # # # ORIGINAL PAYMENT ENDPOINTS
# # # # -------------------------
# # # @app.route("/initiate-payment", methods=["POST"])
# # # def initiate_payment():
# # #     try:
# # #         data = request.json
# # #         phone = data.get("phone")
# # #         amount = data.get("amount")
# # #         if not phone or not amount:
# # #             return jsonify({"error": "Missing phone or amount"}), 400

# # #         deposit_id = str(uuid.uuid4())
# # #         customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# # #         payload = {
# # #             "depositId": deposit_id,
# # #             "amount": str(amount),
# # #             "currency": "ZMW",
# # #             "correspondent": "MTN_MOMO_ZMB",
# # #             "payer": {"type": "MSISDN", "address": {"value": phone}},
# # #             "customerTimestamp": customer_ts,
# # #             "statementDescription": "StudyCraftPay",
# # #             "metadata": [
# # #                 {"fieldName": "orderId", "fieldValue": "ORD-" + deposit_id},
# # #                 {"fieldName": "customerId", "fieldValue": phone, "isPII": True},
# # #             ],
# # #         }

# # #         headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
# # #         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)
# # #         result = {}
# # #         try:
# # #             result = resp.json()
# # #         except Exception:
# # #             logger.warning("Non-JSON response from PawaPay for initiate-payment: %s", resp.text)

# # #         db = get_db()
# # #         db.execute("""
# # #             INSERT OR REPLACE INTO transactions
# # #             (depositId,status,amount,currency,phoneNumber,provider,
# # #              providerTransactionId,failureCode,failureMessage,metadata,received_at,type,user_id)
# # #             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
# # #         """, (
# # #             deposit_id,
# # #             result.get("status", "PENDING"),
# # #             float(amount),
# # #             "ZMW",
# # #             phone,
# # #             None, None, None, None,
# # #             json.dumps(payload["metadata"]),
# # #             datetime.utcnow().isoformat(),
# # #             "payment",
# # #             None
# # #         ))
# # #         db.commit()
# # #         logger.info("initiate-payment: inserted depositId=%s status=%s", deposit_id, result.get("status", "PENDING"))
# # #         return jsonify({"depositId": deposit_id, **result}), 200

# # #     except Exception:
# # #         logger.exception("Payment initiation error")
# # #         return jsonify({"error": "Internal server error"}), 500

# # # # -------------------------
# # # # CALLBACK RECEIVER (upsert-safe for deposits and payouts)
# # # # -------------------------

# # # #Test 2 callback 2
# # # import sqlite3
# # # from flask import Flask, request, jsonify

# # # app = Flask(__name__)

# # # import sqlite3
# # # from flask import Flask, request, jsonify

# # # app = Flask(__name__)

# # # @app.route("/callback/deposit", methods=["POST"])
# # # def deposit_callback():
# # #     try:
# # #         data = request.get_json(force=True)
# # #         print("📩 Full callback data:", data)

# # #         deposit_id = data.get("depositId")
# # #         status = data.get("status", "PENDING").strip().upper()
# # #         amount = data.get("depositedAmount", 0)
# # #         metadata = data.get("metadata", {})
# # #         user_id = metadata.get("userId", "unknown")

# # #         if not deposit_id:
# # #             return jsonify({"error": "Missing depositId"}), 400

# # #         name_of_transaction = f"ZMW{amount} | {user_id} | {deposit_id}"

# # #         db = sqlite3.connect("estack.db")
# # #         db.row_factory = sqlite3.Row
# # #         cur = db.cursor()

# # #         # ✅ Use the correct table (same one as in /initiate)
# # #         cur.execute("""
# # #             CREATE TABLE IF NOT EXISTS estack_transactions (
# # #                 name_of_transaction TEXT NOT NULL,
# # #                 status TEXT NOT NULL
# # #             )
# # #         """)

# # #         # ✅ Check if transaction already exists
# # #         existing = cur.execute(
# # #             "SELECT name_of_transaction FROM estack_transactions WHERE name_of_transaction LIKE ?",
# # #             (f"%{deposit_id}%",)
# # #         ).fetchone()

# # #         if existing:
# # #             cur.execute(
# # #                 "UPDATE estack_transactions SET status = ? WHERE name_of_transaction LIKE ?",
# # #                 (status, f"%{deposit_id}%")
# # #             )
# # #             print(f"🔄 Updated transaction {deposit_id} → {status}")
# # #         else:
# # #             cur.execute(
# # #                 "INSERT INTO estack_transactions (name_of_transaction, status) VALUES (?, ?)",
# # #                 (name_of_transaction, status)
# # #             )
# # #             print(f"💾 Inserted new transaction {deposit_id} → {status}")

# # #         db.commit()
# # #         db.close()

# # #         return jsonify({"success": True, "deposit_id": deposit_id, "status": status}), 200

# # #     except Exception as e:
# # #         print("❌ Error in /callback/deposit:", e)
# # #         return jsonify({"error": str(e)}), 500

# # # -------------------------
# # # DEPOSIT STATUS / TRANSACTION LOOKUP
# # # # -------------------------
# # # @app.route("/deposit_status/<deposit_id>")
# # # def deposit_status(deposit_id):
# # #     db = get_db()
# # #     row = db.execute("SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ?", 
# # #                      (f"%{deposit_id}%",)).fetchone()
# # #     if not row:
# # #         return jsonify({"status": None, "message": "Deposit not found"}), 404
# # #     return jsonify(dict(row)), 200

# # # @app.route("/transactions/<deposit_id>")
# # # def get_transaction(deposit_id):
# # #     db = get_db()
# # #     row = db.execute("SELECT * FROM estack_transactions WHERE name_of_transaction LIKE ?", 
# # #                      (f"%{deposit_id}%",)).fetchone()
# # #     if not row:
# # #         return jsonify({"error": "not found"}), 404
# # #     return jsonify(dict(row)), 200

# # @app.route("/deposit_status/<deposit_id>")
# # def deposit_status(deposit_id):
# #     db = get_db()
# #     row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
# #     if not row:
# #         return jsonify({"status": None, "message": "Deposit not found"}), 404
# #     res = {k: row[k] for k in row.keys()}
# #     if res.get("metadata"):
# #         try:
# #             res["metadata"] = json.loads(res["metadata"])
# #         except:
# #             pass
# #     return jsonify(res), 200

# # @app.route("/transactions/<deposit_id>")
# # def get_transaction(deposit_id):
# #     db = get_db()
# #     row = db.execute("SELECT * FROM transactions WHERE depositId=?", (deposit_id,)).fetchone()
# #     if not row:
# #         return jsonify({"error": "not found"}), 404
# #     res = {k: row[k] for k in row.keys()}
# #     if res.get("metadata"):
# #         try:
# #             res["metadata"] = json.loads(res["metadata"])
# #         except:
# #             pass
# #     return jsonify(res), 200

# # # -------------------------
# # # INVESTMENT ENDPOINTS (Using estack.db)
# # # -------------------------
# # @app.route("/api/investments/initiate", methods=["POST"])
# # def initiate_investment():
# #     try:
# #         data = request.json or {}
# #         phone = data.get("phone") or data.get("phoneNumber")
# #         amount = data.get("amount")
# #         correspondent = data.get("correspondent", "MTN_MOMO_ZMB")
# #         currency = data.get("currency", "ZMW")
# #         user_id = data.get("user_id") or data.get("userId") or "unknown"

# #         if not phone or amount is None:
# #             return jsonify({"error": "Missing phone or amount"}), 400

# #         # Generate a unique deposit ID for this investment
# #         deposit_id = str(uuid.uuid4())
# #         customer_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# #         # Prepare payload for PawaPay (still sending live or test request)
# #         payload = {
# #             "depositId": deposit_id,
# #             "amount": str(amount),
# #             "currency": currency,
# #             "correspondent": correspondent,
# #             "payer": {"type": "MSISDN", "address": {"value": phone}},
# #             "customerTimestamp": customer_ts,
# #             "statementDescription": "Investment",
# #             "metadata": [
# #                 {"fieldName": "purpose", "fieldValue": "investment"},
# #                 {"fieldName": "userId", "fieldValue": str(user_id), "isPII": True},
# #             ],
# #         }

# #         headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
# #         resp = requests.post(PAWAPAY_URL, json=payload, headers=headers)

# #         # Try decoding the response
# #         try:
# #             result = resp.json()
# #         except Exception:
# #             logger.error("PawaPay response not JSON: %s", resp.text)
# #             return jsonify({"error": "Invalid response from PawaPay"}), 502

# #         status = result.get("status", "PENDING")

# #         # Create readable name for transaction
# #         # e.g. "K500 | user_001 | DEP12345"
# #         name_of_transaction = f"{currency}{amount} | {user_id} | {deposit_id}"

# #         # Save to estack.db
# #         db = get_db()
# #         db.execute("""
# #             INSERT INTO estack_transactions (name_of_transaction, status)
# #             VALUES (?, ?)
# #         """, (name_of_transaction, status))
# #         db.commit()

# #         logger.info("💰 Investment initiated: %s (user_id=%s, status=%s)",
# #                     name_of_transaction, user_id, status)

# #         return jsonify({
# #             "message": "Investment initiated successfully",
# #             "depositId": deposit_id,
# #             "user_id": user_id,
# #             "amount": amount,
# #             "status": status
# #         }), 200

# #     except Exception as e:
# #         logger.exception("Investment initiation error")
# #         return jsonify({"error": str(e)}), 500

# # @app.route("/api/investments/user/<user_id>", methods=["GET"])
# # def get_user_investments(user_id):
# #     try:
# #         db = get_db()
# #         rows = db.execute("""
# #             SELECT name_of_transaction, status
# #             FROM estack_transactions
# #             WHERE name_of_transaction LIKE ?
# #             ORDER BY rowid DESC
# #         """, (f"%{user_id}%",)).fetchall()

# #         results = [{"name_of_transaction": r["name_of_transaction"], "status": r["status"]} for r in rows]
# #         return jsonify(results), 200

# #     except Exception as e:
# #         logger.exception("Error fetching user investments")
# #         return jsonify({"error": str(e)}), 500

# # @app.route("/api/investments/status/<deposit_id>", methods=["GET"])
# # def get_investment_status(deposit_id):
# #     try:
# #         db = sqlite3.connect("estack.db")
# #         db.row_factory = sqlite3.Row
# #         cur = db.cursor()

# #         # ✅ Match the same table name
# #         cur.execute("SELECT status FROM estack_transactions WHERE name_of_transaction LIKE ?", (f"%{deposit_id}%",))
# #         row = cur.fetchone()
# #         db.close()

# #         if row:
# #             return jsonify({"status": row["status"]}), 200
# #         else:
# #             return jsonify({"error": "Transaction not found"}), 404

# #     except Exception as e:
# #         print("Error in get_investment_status:", e)
# #         return jsonify({"error": str(e)}), 500

# # # # +++++++++++++++++++++++++++++++++++++++
# # # # Rerieving loans requests
# # # # +++++++++++++++++++++++++++++++++++++++
# # # @app.route("/api/loans/user/<user_id>", methods=["GET"])
# # # def get_user_loans(user_id):
# # #     try:
# # #         db = get_db()
# # #         rows = db.execute("""
# # #             SELECT name, status
# # #             FROM transactions
# # #             WHERE name LIKE ?
# # #             ORDER BY rowid DESC
# # #         """, (f"%{user_id}%",)).fetchall()

# # #         results = []
# # #         for r in rows:
# # #             name = r["name"]
# # #             status = r["status"]

# # #             # Try to split the stored name like: "ZMW500 | user_1 | some-loan-id"
# # #             parts = [p.strip() for p in name.split("|")]
# # #             amount, borrower_id, loan_id = parts if len(parts) == 3 else ("N/A", user_id, "N/A")

# # #             results.append({
# # #                 "loan_id": loan_id,
# # #                 "amount": amount,
# # #                 "status": status,
# # #                 "borrower_id": borrower_id
# # #             })

# # #         return jsonify(results), 200

# # #     except Exception as e:
# # #         logger.exception("Error fetching user loans")
# # #         return jsonify({"error": str(e)}), 500

# # #-----------------------------------
# # # GET PENDING REQUESTS
# # #----------------------------------
# # @app.route("/api/loans/pending", methods=["GET"])
# # def get_pending_loans():
# #     conn = sqlite3.connect(DATABASE)
# #     cur = conn.cursor()
# #     cur.execute("SELECT loanId, user_id, amount, interest, status, expected_return_date FROM loans WHERE status = ?", ("PENDING",))
# #     rows = cur.fetchall()
# #     conn.close()

# #     loans = []
# #     for row in rows:
# #         loans.append({
# #             "loanId": row[0],
# #             "user_id": row[1],
# #             "amount": row[2],
# #             "interest": row[3],
# #             "status": row[4],
# #             "expected_return_date": row[5]
# #         })

# #     return jsonify(loans), 200

# # # # -------------------------
# # # # DISBURSE LOAN (ADMIN ACTION)
# # # # -------------------------

# # # #TEST 4
# # # @app.route("/api/loans/disburse/<loan_id>", methods=["POST"])
# # # def disburse_loan(loan_id):
# # #     try:
# # #         db = get_db()  # ✅ Get the database connection
# # #         data = request.get_json() or {}
# # #         logger.info(f"Disbursing loan {loan_id} with data: {data}")

# # #         # ✅ Fetch loan details (fixed column name)
# # #         loan = db.execute("SELECT * FROM loans WHERE loanId = ?", (loan_id,)).fetchone()
# # #         if not loan:
# # #             return jsonify({"error": "Loan not found"}), 404

# # #         # ✅ Ensure loan is approved before disbursement
# # #         # if loan["status"] != "approved":
# # #         #     return jsonify({"error": "Loan is not approved for disbursement"}), 400

# # #         borrower_id = loan["user_id"]
# # #         amount = float(loan["amount"])

# # #         # ✅ Fetch borrower wallet
# # #         borrower_wallet = db.execute(
# # #             "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
# # #         ).fetchone()
        
# # #         if not borrower_wallet:
# # #             db.execute("""
# # #                 INSERT INTO wallets (user_id, balance, created_at, updated_at)
# # #                 VALUES (?, 0, ?, ?)
# # #             """, (borrower_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat()))
# # #             db.commit()
# # #             logger.info(f"✅ Created new wallet for borrower {borrower_id}")
        
# # #             borrower_wallet = db.execute(
# # #                 "SELECT * FROM wallets WHERE user_id = ?", (borrower_id,)
# # #             ).fetchone()

# # #         borrower_balance = float(borrower_wallet["balance"])

# # #         # ✅ Credit borrower wallet
# # #         new_balance = borrower_balance + amount
# # #         db.execute(
# # #             "UPDATE wallets SET balance = ?, updated_at = ? WHERE user_id = ?",
# # #             (new_balance, datetime.utcnow().isoformat(), borrower_id)
# # #         )

# # #         # ✅ Mark loan as disbursed (fixed column name)
# # #         db.execute(
# # #             "UPDATE loans SET status = 'disbursed', disbursed_at = ? WHERE loanId = ?",
# # #             (datetime.utcnow().isoformat(), loan_id)
# # #         )

# # #         # ✅ Record the disbursement transaction
# # #         db.execute("""
# # #             INSERT INTO transactions (user_id, amount, type, status, reference, created_at, updated_at)
# # #             VALUES (?, ?, 'loan_disbursement', 'SUCCESS', ?, ?, ?)
# # #         """, (
# # #             borrower_id, amount, loan_id,
# # #             datetime.utcnow().isoformat(),
# # #             datetime.utcnow().isoformat()
# # #         ))

# # #         db.commit()

# # #         # ✅ Link this loan to one available investment
# # #         try:
# # #             # investment_row = db.execute("""
# # #             #     SELECT depositId FROM transactions
# # #             #     WHERE type = 'investment' AND status = 'ACTIVE'
# # #             #     ORDER BY received_at ASC LIMIT 1
# # #             # """).fetchone()
# # #             investment_row = db.execute("""
# # #                 SELECT reference FROM transactions
# # #                 WHERE type = 'investment' AND status = 'ACTIVE'
# # #                 ORDER BY created_at ASC LIMIT 1
# # #             """).fetchone()


# # #             if investment_row:
# # #                 # investment_id = investment_row["depositId"]
# # #                 investment_id = investment_row["reference"]


# # #                 # ✅ Mark that single investment as LOANED_OUT
# # #                 db.execute("""
# # #                     UPDATE transactions
# # #                     SET status = 'LOANED_OUT', updated_at = ?
# # #                     WHERE reference = ?

# # #                     # UPDATE transactions
# # #                     # SET status = 'LOANED_OUT', investment_id = ?, updated_at = ?
# # #                     # WHERE depositId = ?
# # #                 """, (loan_id, datetime.utcnow().isoformat(), investment_id))
# # #                 db.commit()

# # #                 # ✅ Notify the investor
# # #                 investor_row = db.execute("""
# # #                     SELECT user_id FROM transactions
# # #                     WHERE reference = ? AND type = 'investment'
# # #                     # SELECT user_id FROM transactions
# # #                     # WHERE depositId = ? AND type = 'investment'
# # #                 """, (investment_id,)).fetchone()
# # #                 # investment_row = db.execute("""
# # #                 #     SELECT reference FROM transactions
# # #                 #     WHERE type = 'investment' AND status = 'ACTIVE'
# # #                 #     ORDER BY created_at ASC LIMIT 1
# # #                 # """).fetchone()

# # #                 # ✅ Also mark investor's transaction as DISBURSED
# # #                 if loan["investment_id"]:
# # #                     db.execute("""
# # #                         UPDATE transactions
# # #                         SET status = 'DISBURSED', updated_at = ?
# # #                         WHERE depositId = ?
# # #                     """, (datetime.utcnow().isoformat(), loan["investment_id"]))
# # #                     logger.info(f"✅ Investor transaction {loan['investment_id']} marked as DISBURSED.")



# # #                 if investor_row and investor_row["user_id"]:
# # #                     notify_investor(
# # #                         investor_row["user_id"],
# # #                         f"Your investment {investment_id[:8]} has been loaned out to borrower {loan_id[:8]}."
# # #                     )

# # #                 logger.info(f"Investment {investment_id} linked to loan {loan_id}")
# # #             else:
# # #                 logger.warning("No available active investment found to link with this loan.")

# # #         except Exception as e:
# # #             logger.error(f"Error linking investment to loan {loan_id}: {e}")

# # #         return jsonify({
# # #             "message": f"Loan {loan_id} successfully disbursed",
# # #             "borrower_id": borrower_id,
# # #             "amount": amount,
# # #             "new_balance": new_balance
# # #         }), 200

# # #     except Exception as e:
# # #         logger.error(f"Error disbursing loan {loan_id}: {e}")
# # #         return jsonify({"error": str(e)}), 500

# # # # -------------------------
# # # # REJECT LOAN (ADMIN ACTION)
# # # # -------------------------
# # # @app.route("/api/loans/reject/<loan_id>", methods=["POST"])
# # # def reject_loan(loan_id):
# # #     admin_id = request.json.get("admin_id", "admin_default")
# # #     db = get_db()
# # #     loan = db.execute("SELECT * FROM loans WHERE loanId=?", (loan_id,)).fetchone()
# # #     if not loan:
# # #         return jsonify({"error": "Loan not found"}), 404
# # #     if loan["status"] != "PENDING":
# # #         return jsonify({"error": f"Loan already {loan['status']}"}), 400

# # #     db.execute("UPDATE loans SET status='REJECTED', approved_by=? WHERE loanId=?", (admin_id, loan_id))
# # #     db.commit()

# # #     return jsonify({"loanId": loan_id, "status": "REJECTED"}), 200

# # # # OPTIONAL CODE CHECK NOTIFICATION 
# # # @app.route("/api/notifications/<user_id>", methods=["GET"])
# # # def get_notifications(user_id):
# # #     conn = sqlite3.connect(DATABASE)
# # #     conn.row_factory = sqlite3.Row
# # #     cur = conn.cursor()
# # #     cur.execute("SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC", (user_id,))
# # #     rows = cur.fetchall()
# # #     conn.close()
# # #     return jsonify([dict(row) for row in rows]), 200

# # # # -------------------------
# # # # RUN
# # # # -------------------------

# # # if __name__ == "__main__":
# # #     with app.app_context():
# # #         init_db()
# # #     port = int(os.environ.get("PORT", 5000))
# # #     app.run(host="0.0.0.0", port=port)










































