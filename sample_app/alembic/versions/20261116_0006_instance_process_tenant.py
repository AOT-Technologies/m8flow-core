#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#

"""Enforce tenant ownership of an instance's BPMN process."""

import sqlalchemy as sa
from alembic import op

revision = "20261116_0006"
down_revision = "20261112_0005"
branch_labels = None
depends_on = None

_INSTANCE = "process_instance"
_PROCESS = "bpmn_process"
_INDEX = "m8f_bpmn_process_tenant_id_key"
_FK = "fk_process_instance_process_tenant"
_INSTANCE_COLUMNS = ["m8f_tenant_id", "bpmn_process_id"]
_PROCESS_COLUMNS = ["m8f_tenant_id", "id"]


def upgrade() -> None:
    bind = op.get_bind()
    invalid_id = bind.scalar(
        sa.text(
            "SELECT i.id FROM process_instance AS i "
            "LEFT JOIN bpmn_process AS p "
            "ON p.id = i.bpmn_process_id AND p.m8f_tenant_id = i.m8f_tenant_id "
            "WHERE i.bpmn_process_id IS NOT NULL AND p.id IS NULL "
            "ORDER BY i.id LIMIT 1"
        )
    )
    if invalid_id is not None:
        raise RuntimeError(
            f"Process instance {invalid_id} references a missing BPMN process "
            "or one owned by another tenant. Correct the process reference "
            "before retrying this migration."
        )

    # The sample app's ORM baseline may already have these objects.
    inspector = sa.inspect(bind)
    if _INDEX not in {index["name"] for index in inspector.get_indexes(_PROCESS)}:
        op.create_index(_INDEX, _PROCESS, _PROCESS_COLUMNS, unique=True)
    if _FK not in {key["name"] for key in inspector.get_foreign_keys(_INSTANCE)}:
        with op.batch_alter_table(_INSTANCE) as batch:
            batch.create_foreign_key(_FK, _PROCESS, _INSTANCE_COLUMNS, _PROCESS_COLUMNS)


def downgrade() -> None:
    with op.batch_alter_table(_INSTANCE) as batch:
        batch.drop_constraint(_FK, type_="foreignkey")
    op.drop_index(_INDEX, table_name=_PROCESS)
