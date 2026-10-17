from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, String, func, text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Orders(Base):
    """Application-layer transactional model mimicking a production e-commerce backend"""

    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, unique=True, nullable=False)
    customer_id = Column(Integer, nullable=True, index=True)
    sku = Column(String(50), nullable=False)
    product_description = Column(String(500), nullable=True)
    price = Column(Numeric(10, 2), nullable=False)
    currency_code = Column(String(3), nullable=True)
    qty = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    completed_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), nullable=False)
    revision = Column(Integer, server_default=text("1"), nullable=False)

    def __repr__(self):
        return f"<Order(order_id={self.order_id}, sku='{self.sku}')>"


class Refunds(Base):
    """A recorded refund transaction for an order."""

    __tablename__ = "refunds"

    refund_id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(
        Integer, ForeignKey("orders.order_id", ondelete="CASCADE"), nullable=False
    )
    refund_amount = Column(Numeric(10, 2), nullable=False)
    currency_code = Column(String(3), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
