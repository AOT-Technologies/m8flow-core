"""Add waiting message subscriptions and delivery receipts."""

import sqlalchemy as sa
from alembic import op

revision = "m8f_694_message_correlation"
down_revision = "m8f_671_compensation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "message_subscription",
        sa.Column(
            "m8f_tenant_id",
            sa.String(255),
            sa.ForeignKey("m8flow_tenant.id"),
            primary_key=True,
        ),
        sa.Column("process_instance_id", sa.Integer(), primary_key=True),
        sa.Column("task_guid", sa.String(36), primary_key=True),
        sa.Column("process_definition_id", sa.Integer(), nullable=False),
        sa.Column("message_name", sa.String(255), nullable=False),
        sa.Column("property_paths", sa.Text(), nullable=False),
        sa.Column("correlation_digest", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(
            ["m8f_tenant_id", "process_instance_id"],
            ["process_instance.m8f_tenant_id", "process_instance.id"],
            ondelete="CASCADE",
            name="m8f_msg_sub_instance_fk",
        ),
        sa.ForeignKeyConstraint(
            ["m8f_tenant_id", "process_definition_id"],
            ["bpmn_process_definition.m8f_tenant_id", "bpmn_process_definition.id"],
            name="m8f_msg_sub_definition_fk",
        ),
    )
    op.create_index(
        "m8f_msg_sub_match",
        "message_subscription",
        ["m8f_tenant_id", "message_name", "correlation_digest"],
    )
    op.create_index(
        "m8f_msg_sub_definition",
        "message_subscription",
        ["m8f_tenant_id", "message_name", "process_definition_id"],
    )
    op.create_table(
        "message_delivery_receipt",
        sa.Column(
            "m8f_tenant_id",
            sa.String(255),
            sa.ForeignKey("m8flow_tenant.id"),
            primary_key=True,
        ),
        sa.Column("delivery_id", sa.String(255), primary_key=True),
        sa.Column("request_digest", sa.String(64), nullable=False),
        sa.Column("process_instance_id", sa.Integer()),
        sa.Column("task_guid", sa.String(36)),
        sa.Column("outcome", sa.String(20)),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "(outcome IS NULL AND process_instance_id IS NULL "
            "AND task_guid IS NULL) OR "
            "(outcome IS NOT NULL AND outcome IN ('delivered', 'workflow_error') "
            "AND process_instance_id IS NOT NULL "
            "AND task_guid IS NOT NULL)",
            name="m8f_msg_receipt_outcome",
        ),
    )


def downgrade() -> None:
    op.drop_table("message_delivery_receipt")
    op.drop_table("message_subscription")
