"""add customer identity, update tracking and refunds

Revision ID: 51acb06b7f2e
Revises: 009bbb520fd6
Create Date: 2026-10-13
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "51acb06b7f2e"
down_revision: Union[str, Sequence[str], None] = "009bbb520fd6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("orders", sa.Column("customer_id", sa.Integer(), nullable=True))
    op.add_column(
        "orders",
        sa.Column("updated_at", sa.DateTime(), nullable=True),
    )
    op.execute("UPDATE orders SET updated_at = created_at")
    op.alter_column(
        "orders",
        "updated_at",
        existing_type=sa.DateTime(),
        server_default=sa.text("now()"),
        nullable=False,
    )
    op.add_column(
        "orders", sa.Column("completed_at", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "orders", sa.Column("currency_code", sa.String(length=3), nullable=True)
    )
    op.add_column(
        "orders",
        sa.Column(
            "revision",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
    )
    op.create_index("ix_orders_customer_id", "orders", ["customer_id"])
    op.create_table(
        "refunds",
        sa.Column("refund_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("order_id", sa.Integer(), nullable=False),
        sa.Column("refund_amount", sa.Numeric(10, 2), nullable=False),
        sa.Column("currency_code", sa.String(length=3), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["order_id"], ["orders.order_id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("refund_id"),
    )
    op.create_index("ix_refunds_created_at", "refunds", ["created_at"])

    op.execute(
        """
        CREATE FUNCTION set_order_timestamps() RETURNS trigger AS $$
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                NEW.updated_at = now();
                NEW.revision = OLD.revision + 1;
                IF NEW.status = 'COMPLETED'
                      AND OLD.status IS DISTINCT FROM 'COMPLETED' THEN
                    NEW.completed_at = now();
                ELSIF NEW.status <> 'COMPLETED'
                      AND OLD.status = 'COMPLETED' THEN
                    NEW.completed_at = OLD.completed_at;
                END IF;
            ELSIF NEW.updated_at IS NULL THEN
                NEW.updated_at = now();
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER orders_updated_at
        BEFORE INSERT OR UPDATE ON orders
        FOR EACH ROW EXECUTE FUNCTION set_order_timestamps()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER orders_updated_at ON orders")
    op.execute("DROP FUNCTION set_order_timestamps()")
    op.drop_index("ix_refunds_created_at", table_name="refunds")
    op.drop_table("refunds")
    op.drop_index("ix_orders_customer_id", table_name="orders")
    op.drop_column("orders", "revision")
    op.drop_column("orders", "currency_code")
    op.drop_column("orders", "updated_at")
    op.drop_column("orders", "completed_at")
    op.drop_column("orders", "customer_id")
