import json
import random
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from data_infra.integrations.shop.product_aliases import (
    PRODUCT_DESCRIPTION_ALIASES as PRODUCT_ALIAS,
)

_CATALOG_PATH = Path(__file__).resolve().parents[2] / "static" / "product_catalog.json"

PRODUCT_CATALOG: list[dict] = json.loads(_CATALOG_PATH.read_text())
PRODUCT_DESCRIPTIONS: dict[str, str] = {
    product["sku"]: product["description"] for product in PRODUCT_CATALOG
}
PRODUCT_PRICES: dict[str, float] = {
    product["sku"]: product["price"] for product in PRODUCT_CATALOG
}
PRODUCT_SKUS: set[str] = {product["sku"] for product in PRODUCT_CATALOG}

STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED", "PENDING"]


def _product_description(sku: str) -> str:
    aliases = [
        alias.strip()
        for alias in PRODUCT_ALIAS[sku]
        if isinstance(alias, str) and alias.strip()
    ]
    if aliases:
        return random.choice(aliases)

    description = PRODUCT_DESCRIPTIONS[sku].strip()
    if not description:
        raise ValueError(f"No non-empty product description configured for SKU {sku}")
    return description


def generate_order_records(days: int = 7) -> list[dict]:
    """Generate order records with stable customer identities and change timestamps."""
    start_date = datetime.now() - timedelta(days=days)
    current_order_id = 10001
    records = []

    for day_offset in range(days):
        target_date = start_date + timedelta(days=day_offset)
        day_name = target_date.strftime("%A")

        daily_volume = (
            random.randint(32000, 36000)
            if day_name == "Friday"
            else random.randint(9500, 10500)
        )

        for order_index in range(daily_volume):
            sku = random.choice(list(PRODUCT_PRICES.keys()))
            status = "COMPLETED" if order_index == 0 else random.choice(STATUSES)
            qty = 0 if status == "CANCELLED" else random.randint(1, 4)
            order_time = target_date.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )
            description = _product_description(sku)
            records.append(
                {
                    "order_id": current_order_id,
                    "customer_id": current_order_id % 10000 + 1,
                    "sku": sku,
                    "product_description": description,
                    "price": Decimal(str(PRODUCT_PRICES[sku])),
                    "currency_code": "USD",
                    "qty": qty,
                    "status": status,
                    "created_at": order_time,
                    "completed_at": order_time if status == "COMPLETED" else None,
                    "updated_at": order_time,
                }
            )
            current_order_id += 1

    return records


def ensure_daily_refund_coverage(
    orders: list[dict], refund_records: list[dict]
) -> list[dict]:
    """Ensure at least one refund event for every order date with a completed order."""
    refunded_dates = {refund["created_at"].date() for refund in refund_records}
    refunded_order_ids = {refund["order_id"] for refund in refund_records}
    completed_orders_by_date: dict = {}

    for order in orders:
        if order["status"] == "COMPLETED":
            order_date = order["created_at"].date()
            completed_orders_by_date.setdefault(order_date, []).append(order)

    order_dates = {order["created_at"].date() for order in orders}
    for order_date in sorted(order_dates - refunded_dates):
        completed_orders = completed_orders_by_date.get(order_date, [])
        if not completed_orders:
            raise ValueError(
                f"Cannot generate a refund for {order_date}: "
                "there is no completed order on that date."
            )

        refund_order = next(
            (
                order
                for order in completed_orders
                if order["order_id"] not in refunded_order_ids
            ),
            completed_orders[0],
        )
        refund_records.append(
            {
                "order_id": refund_order["order_id"],
                "refund_amount": refund_order["price"] * refund_order["qty"],
                "currency_code": refund_order.get("currency_code"),
                "created_at": refund_order["created_at"],
            }
        )
        refunded_order_ids.add(refund_order["order_id"])

    return refund_records


def generate_refund_records(orders: list[dict]) -> list[dict]:
    """Generate explicit full-refund events for a small sample of completed orders."""
    refund_records = ensure_daily_refund_coverage(orders, [])
    covered_order_ids = {refund["order_id"] for refund in refund_records}

    for order in orders:
        if (
            order["status"] != "COMPLETED"
            or order["order_id"] in covered_order_ids
            or random.random() >= 0.05
        ):
            continue

        refund_records.append(
            {
                "order_id": order["order_id"],
                "refund_amount": order["price"] * order["qty"],
                "currency_code": order.get("currency_code"),
                "created_at": order["created_at"] + timedelta(days=random.randint(1, 30)),
            }
        )
        covered_order_ids.add(order["order_id"])
    return refund_records


if __name__ == "__main__":
    orders = generate_order_records()
    generate_refund_records(orders)
