#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#

"""Merge the collaboration and compensation migration heads."""

revision = "m8f_merge_runtime"
down_revision = ("m8f_collaboration_01", "compensation_journal")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
