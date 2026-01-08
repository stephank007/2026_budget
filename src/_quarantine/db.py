# src/services/db.py

from __future__ import annotations

from typing import Optional

import pandas as pd
from pymongo import MongoClient, InsertOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError

# -------------------------------------------------------------------
#  Central DB constants
# -------------------------------------------------------------------

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "Expenses"
TRANSACTIONS_COLLECTION = "transactions"


# -------------------------------------------------------------------
#  Connection helpers
# -------------------------------------------------------------------

def get_client(uri: str = MONGO_URI) -> MongoClient:
    """Return a MongoClient. Caller is responsible for keeping it alive."""
    return MongoClient(uri)


def get_db(client: Optional[MongoClient] = None,
           db_name: str = DB_NAME) -> Database:
    """Return the Expenses database."""
    if client is None:
        client = get_client()
    return client[db_name]


def get_transactions_collection(db: Optional[Database] = None) -> Collection:
    """Return the main transactions collection."""
    if db is None:
        db = get_db()
    return db[TRANSACTIONS_COLLECTION]


# -------------------------------------------------------------------
#  Indexes
# -------------------------------------------------------------------

def ensure_transactions_indexes(coll: Optional[Collection] = None) -> None:
    """
    Ensure the unique index for deduplication exists.
    Safe to call multiple times.
    """
    if coll is None:
        coll = get_transactions_collection()
    coll.create_index("txn_hash", unique=True)


# -------------------------------------------------------------------
#  Inserts
# -------------------------------------------------------------------

def insert_transactions_df(df: pd.DataFrame,
                           coll: Optional[Collection] = None) -> None:
    """
    Insert a DataFrame of transactions into MongoDB.

    Expects df to already contain a 'txn_hash' column.
    Duplicate txn_hash rows will be ignored thanks to the unique index.
    """
    if df.empty:
        print("  [INFO] insert_transactions_df: nothing to insert (empty df).")
        return
    
    if "txn_hash" not in df.columns:
        raise ValueError("DataFrame must contain a 'txn_hash' column before insert.")
    
    if coll is None:
        coll = get_transactions_collection()
    
    records = df.to_dict("records")
    ops = [InsertOne(r) for r in records]
    
    print(f"  [INFO] Inserting {len(ops)} transactions into MongoDB...")
    try:
        result = coll.bulk_write(ops, ordered=False)
        inserted = getattr(result, "inserted_count", "unknown")
        print(f"  [OK] Inserted {inserted} new transactions.\n")
    except BulkWriteError:
        # Duplicate key errors are expected on re-runs due to unique txn_hash
        print("  [WARN] Bulk write completed with duplicate key errors (existing rows skipped).\n")

