from tests.fixtures import data_generator


def test_product_description_falls_back_when_aliases_are_blank(monkeypatch):
    sku = next(iter(data_generator.PRODUCT_ALIAS))
    monkeypatch.setitem(data_generator.PRODUCT_ALIAS, sku, ["", "  "])

    expected = data_generator.PRODUCT_DESCRIPTIONS[sku]
    assert data_generator._product_description(sku) == expected


def test_refunds_are_only_generated_for_completed_orders(monkeypatch):
    monkeypatch.setattr(data_generator.random, "random", lambda: 1.0)
    monkeypatch.setattr(data_generator.random, "randint", lambda _start, _end: 2)
    created_at = data_generator.datetime(2026, 10, 1)
    orders = [
        {
            "order_id": 1,
            "status": "COMPLETED",
            "created_at": created_at,
            "completed_at": created_at,
            "price": data_generator.Decimal("12.50"),
            "qty": 2,
            "currency_code": "USD",
        },
        {
            "order_id": 2,
            "status": "PENDING",
            "created_at": created_at,
            "completed_at": None,
            "price": data_generator.Decimal("99.00"),
            "qty": 1,
            "currency_code": "USD",
        },
        {
            "order_id": 3,
            "status": "CANCELLED",
            "created_at": created_at,
            "completed_at": None,
            "price": data_generator.Decimal("50.00"),
            "qty": 0,
            "currency_code": "USD",
        },
    ]

    refunds = data_generator.generate_refund_records(orders)

    assert refunds == [
        {
            "order_id": 1,
            "refund_amount": data_generator.Decimal("25.00"),
            "currency_code": "USD",
            "created_at": created_at,
        }
    ]


def test_refund_generator_covers_every_order_date(monkeypatch):
    monkeypatch.setattr(data_generator.random, "random", lambda: 1.0)
    first_date = data_generator.datetime(2026, 10, 6)
    orders = [
        {
            "order_id": 1,
            "status": "COMPLETED",
            "created_at": first_date,
            "price": data_generator.Decimal("10.00"),
            "qty": 2,
            "currency_code": "USD",
        },
        {
            "order_id": 2,
            "status": "COMPLETED",
            "created_at": first_date + data_generator.timedelta(days=1),
            "price": data_generator.Decimal("15.00"),
            "qty": 1,
            "currency_code": "USD",
        },
    ]

    refunds = data_generator.generate_refund_records(orders)

    assert {refund["created_at"].date() for refund in refunds} == {
        first_date.date(),
        (first_date + data_generator.timedelta(days=1)).date(),
    }
    assert [refund["order_id"] for refund in refunds] == [1, 2]
