"""Waiting message subscriptions and committed delivery receipts."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from m8flow_core.models.base import Base
from m8flow_core.models.tenant_scoped import M8fTenantScopedMixin, TenantScoped


class MessageSubscriptionModel(M8fTenantScopedMixin, TenantScoped, Base):
    __tablename__ = "message_subscription"
    __table_args__ = (
        ForeignKeyConstraint(
            ["m8f_tenant_id", "process_instance_id"],
            ["process_instance.m8f_tenant_id", "process_instance.id"],
            ondelete="CASCADE",
            name="m8f_msg_sub_instance_fk",
        ),
        ForeignKeyConstraint(
            ["m8f_tenant_id", "process_definition_id"],
            ["bpmn_process_definition.m8f_tenant_id", "bpmn_process_definition.id"],
            name="m8f_msg_sub_definition_fk",
        ),
        Index(
            "m8f_msg_sub_match", "m8f_tenant_id", "message_name", "correlation_digest"
        ),
        Index(
            "m8f_msg_sub_definition",
            "m8f_tenant_id",
            "message_name",
            "process_definition_id",
        ),
    )

    m8f_tenant_id: Mapped[str] = mapped_column(
        ForeignKey("m8flow_tenant.id"),
        primary_key=True,
    )
    process_instance_id: Mapped[int] = mapped_column(primary_key=True)
    task_guid: Mapped[str] = mapped_column(String(36), primary_key=True)
    process_definition_id: Mapped[int] = mapped_column()
    message_name: Mapped[str] = mapped_column(String(255))
    property_paths: Mapped[str] = mapped_column(Text)
    correlation_digest: Mapped[str] = mapped_column(String(64))


class MessageDeliveryReceiptModel(M8fTenantScopedMixin, TenantScoped, Base):
    __tablename__ = "message_delivery_receipt"
    __table_args__ = (
        CheckConstraint(
            "(outcome IS NULL AND process_instance_id IS NULL "
            "AND task_guid IS NULL) OR "
            "(outcome IS NOT NULL AND outcome IN ('delivered', 'workflow_error') "
            "AND process_instance_id IS NOT NULL "
            "AND task_guid IS NOT NULL)",
            name="m8f_msg_receipt_outcome",
        ),
    )

    m8f_tenant_id: Mapped[str] = mapped_column(
        ForeignKey("m8flow_tenant.id"),
        primary_key=True,
    )
    delivery_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    request_digest: Mapped[str] = mapped_column(String(64))
    # Retain idempotency when the host purges a finished process instance.
    process_instance_id: Mapped[int | None] = mapped_column()
    task_guid: Mapped[str | None] = mapped_column(String(36))
    outcome: Mapped[str | None] = mapped_column(String(20))
    _delivered_at: Mapped[datetime] = mapped_column(
        "delivered_at", DateTime(timezone=True)
    )

    @property
    def delivered_at(self) -> datetime:
        if self._delivered_at.tzinfo is None:
            return self._delivered_at.replace(tzinfo=UTC)
        return self._delivered_at.astimezone(UTC)
