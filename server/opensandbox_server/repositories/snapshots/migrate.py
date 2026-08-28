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

Repository imports are deferred to call time: the ``services`` package eagerly
imports the Docker and Kubernetes service stack, which transitively imports
this repository package, so top-level imports here would create an import
cycle when the CLI entry point loads.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DEFAULT_SQLITE_SNAPSHOT_PATH = Path.home() / ".opensandbox" / "opensandbox.db"

_MIGRATION_PAGE_SIZE = 100


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

    The PostgreSQL schema is created when needed. Records whose id already
    exists in PostgreSQL are skipped, so the command can be re-run safely.
    When ``dry_run`` is True no writes occur and the result counts the records
    that would be migrated.

    Args:
        sqlite_path: Path to the source SQLite database file.
        postgresql_dsn: PostgreSQL connection string for the target database.
        dry_run: Report what would be migrated without writing anything.

    Returns:
        A SnapshotMigrationResult with the record counts.

    Raises:
        FileNotFoundError: If the SQLite database file does not exist.
    """
    from opensandbox_server.repositories.snapshots.postgresql import (
        PostgreSQLSnapshotRepository,
    )
    from opensandbox_server.repositories.snapshots.sqlite import SQLiteSnapshotRepository
    from opensandbox_server.services.snapshot_repository import SnapshotListQuery

    source_path = Path(sqlite_path).expanduser()
    if not source_path.is_file():
        raise FileNotFoundError(f"SQLite snapshot database not found: {source_path}")

    sqlite_repo: SQLiteSnapshotRepository | None = None
    postgresql_repo: PostgreSQLSnapshotRepository | None = None
    try:
        sqlite_repo = SQLiteSnapshotRepository(source_path)
        postgresql_repo = PostgreSQLSnapshotRepository(postgresql_dsn)
        total = 0
        migrated = 0
        skipped = 0
        page = 1
        while True:
            result = sqlite_repo.list(
                SnapshotListQuery(page=page, page_size=_MIGRATION_PAGE_SIZE)
            )
            if not result.items:
                break
            for record in result.items:
                total += 1
                if postgresql_repo.get(record.id) is not None:
                    skipped += 1
                    continue
                migrated += 1
                if dry_run:
                    continue
                postgresql_repo.create(record)
            if len(result.items) < _MIGRATION_PAGE_SIZE:
                break
            page += 1
    finally:
        if sqlite_repo is not None:
            sqlite_repo.close()
        if postgresql_repo is not None:
            postgresql_repo.close()

    return SnapshotMigrationResult(
        total=total,
        migrated=migrated,
        skipped=skipped,
        dry_run=dry_run,
    )


__all__ = [
    "DEFAULT_SQLITE_SNAPSHOT_PATH",
    "SnapshotMigrationResult",
    "migrate_sqlite_snapshots_to_postgresql",
]
