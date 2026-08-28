# Copyright 2026 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
One-shot migration of snapshot records from SQLite to PostgreSQL.

The PostgreSQL backend added in the snapshot store feature is opt-in, and it
does not read existing SQLite databases. Operators who switch a running server
from ``store.type = "sqlite"`` to ``store.type = "postgresql"`` use this module
to copy the persisted snapshot catalog before restarting the server.

The source SQLite database is opened read-only and its schema is never
modified. Dry runs only inspect the target: the PostgreSQL schema is created
only on a real migration run.

Repository imports are deferred to call time: the ``services`` package eagerly
imports the Docker and Kubernetes service stack, which transitively imports
this repository package, so top-level imports here would create an import
cycle when the CLI entry point loads.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opensandbox_server.services.snapshot_models import SnapshotRecord

DEFAULT_SQLITE_SNAPSHOT_PATH = Path.home() / ".opensandbox" / "opensandbox.db"

# Columns stored by the snapshot repository. Older databases may predate the
# namespace column, so each selected column is included only when present.
_SNAPSHOT_TABLE_COLUMNS = (
    "id",
    "source_sandbox_id",
    "namespace",
    "name",
    "description",
    "restore_config",
    "state",
    "reason",
    "message",
    "last_transition_at",
    "created_at",
    "updated_at",
)


@dataclass(slots=True)
class SnapshotMigrationResult:
    """Counts for a completed SQLite-to-PostgreSQL migration run."""

    total: int
    migrated: int
    skipped: int
    dry_run: bool


def migrate_sqlite_snapshots_to_postgresql(
    sqlite_path: str | Path,
    postgresql_dsn: str,
    *,
    dry_run: bool = False,
) -> SnapshotMigrationResult:
    """
    Copy snapshot records from a SQLite database into PostgreSQL.

    The source SQLite database is opened read-only and is never modified.
    The PostgreSQL schema is created only when a real migration run needs it;
    a dry run inspects the target without creating or altering anything.
    Records whose id already exists in PostgreSQL are skipped, so the command
    can be re-run safely.

    Args:
        sqlite_path: Path to the source SQLite database file.
        postgresql_dsn: PostgreSQL connection string for the target database.
        dry_run: Report what would be migrated without writing anything.

    Returns:
        A SnapshotMigrationResult with the record counts.

    Raises:
        FileNotFoundError: If the SQLite database file does not exist.
    """
    source_path = Path(sqlite_path).expanduser()
    if not source_path.is_file():
        raise FileNotFoundError(f"SQLite snapshot database not found: {source_path}")

    records = _read_sqlite_snapshots_read_only(source_path)
    if dry_run:
        existing_ids = _read_postgresql_snapshot_ids(postgresql_dsn)
        migrated = sum(1 for record in records if record.id not in existing_ids)
        return SnapshotMigrationResult(
            total=len(records),
            migrated=migrated,
            skipped=len(records) - migrated,
            dry_run=True,
        )

    from opensandbox_server.repositories.snapshots.postgresql import (
        PostgreSQLSnapshotRepository,
    )

    postgresql_repo = PostgreSQLSnapshotRepository(postgresql_dsn)
    try:
        existing_ids = _read_postgresql_snapshot_ids(postgresql_dsn)
        migrated = 0
        for record in records:
            if record.id in existing_ids:
                continue
            postgresql_repo.create(record)
            migrated += 1
    finally:
        postgresql_repo.close()

    return SnapshotMigrationResult(
        total=len(records),
        migrated=migrated,
        skipped=len(records) - migrated,
        dry_run=False,
    )


def _read_sqlite_snapshots_read_only(db_path: Path) -> list[SnapshotRecord]:
    """
    Read every snapshot record without initializing or modifying the schema.

    The database is opened with SQLite's read-only URI mode so a backup on a
    read-only mount can be migrated, and databases that predate the namespace
    column are read with the missing column defaulting to None.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        table_info = conn.execute("PRAGMA table_info(snapshots)").fetchall()
        columns = {row["name"] for row in table_info}
        if not columns:
            return []
        selected = [column for column in _SNAPSHOT_TABLE_COLUMNS if column in columns]
        rows = conn.execute(
            f"SELECT {', '.join(selected)} FROM snapshots ORDER BY created_at DESC, id DESC"
        ).fetchall()
        records: list[SnapshotRecord] = []
        for row in rows:
            values: dict[str, Any] = {column: row[column] for column in selected}
            for column in _SNAPSHOT_TABLE_COLUMNS:
                values.setdefault(column, None)
            records.append(_row_to_record(values))
        return records
    finally:
        conn.close()


def _row_to_record(values: dict[str, Any]) -> SnapshotRecord:
    from opensandbox_server.services.snapshot_models import (
        SnapshotRecord,
        SnapshotRestoreConfig,
        SnapshotState,
        SnapshotStatusRecord,
    )

    restore_config = json.loads(values["restore_config"])
    return SnapshotRecord(
        id=values["id"],
        source_sandbox_id=values["source_sandbox_id"],
        namespace=values["namespace"],
        name=values["name"],
        description=values["description"],
        restore_config=SnapshotRestoreConfig.from_dict(restore_config),
        status=SnapshotStatusRecord(
            state=SnapshotState(values["state"]),
            reason=values["reason"],
            message=values["message"],
            last_transition_at=_str_to_datetime(values["last_transition_at"]),
        ),
        created_at=_require_datetime(values["created_at"]),
        updated_at=_require_datetime(values["updated_at"]),
    )


def _str_to_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _require_datetime(value: str | None) -> datetime:
    result = _str_to_datetime(value)
    if result is None:
        raise ValueError("snapshot row is missing a required timestamp")
    return result


def _read_postgresql_snapshot_ids(dsn: str) -> set[str]:
    """
    Return the ids already present in the target snapshot table.

    Read-only: the schema is not created or altered here. A target without
    the snapshot table contributes no existing ids, so a dry run against it
    reports every source record as migratable.
    """
    import psycopg

    with psycopg.connect(dsn) as conn:
        row = conn.execute("SELECT to_regclass('snapshots')").fetchone()
        if row is None or row[0] is None:
            return set()
        ids = conn.execute("SELECT id FROM snapshots").fetchall()
        return {item[0] for item in ids}


__all__ = [
    "DEFAULT_SQLITE_SNAPSHOT_PATH",
    "SnapshotMigrationResult",
    "migrate_sqlite_snapshots_to_postgresql",
]
