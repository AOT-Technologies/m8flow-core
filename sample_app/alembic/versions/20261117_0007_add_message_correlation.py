"""Add the library's message correlation tables to the sample app schema."""

from alembic import op
from m8flow_core.models import Base as BpmnCoreBase

revision = "20261117_0007"
down_revision = "20261116_0006"
branch_labels = None
depends_on = None

TABLE_NAMES = ("message_subscription", "message_delivery_receipt")


def upgrade() -> None:
    bind = op.get_bind()
    for name in TABLE_NAMES:
        BpmnCoreBase.metadata.tables[name].create(bind=bind, checkfirst=True)


def downgrade() -> None:
    bind = op.get_bind()
    for name in reversed(TABLE_NAMES):
        BpmnCoreBase.metadata.tables[name].drop(bind=bind, checkfirst=True)
