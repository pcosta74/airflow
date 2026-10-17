import pytest

from data_infra.integrations.pim.catalog import (
    product_catalog_to_dimension_records,
    validate_product_catalog,
)
from tests.fixtures.data_generator import (
    PRODUCT_CATALOG,
    PRODUCT_DESCRIPTIONS,
    PRODUCT_PRICES,
    PRODUCT_SKUS,
)


def test_catalog_and_fixture_generator_use_the_same_catalog():
    products = validate_product_catalog(PRODUCT_CATALOG)
    assert {product["sku"] for product in products} == PRODUCT_SKUS
    for product in products:
        sku = product["sku"]
        assert product["source_description"] == PRODUCT_DESCRIPTIONS[sku]
        assert product["canonical_description"] == PRODUCT_DESCRIPTIONS[sku]
        assert product["price"] == PRODUCT_PRICES[sku]


def test_catalog_dimension_records_use_only_canonical_description():
    products = validate_product_catalog(PRODUCT_CATALOG)

    first = product_catalog_to_dimension_records(products)
    second = product_catalog_to_dimension_records(products)

    assert len(first) == len(products)
    assert first == second
    for product, dimension in zip(products, first, strict=True):
        assert dimension["sku"] == product["sku"]
        assert dimension["source_description"] == product["source_description"]
        assert dimension["canonical_description"] == product["canonical_description"]
        assert dimension["price"] == product["price"]


def test_duplicate_normalized_skus_are_rejected():
    product = validate_product_catalog(PRODUCT_CATALOG)[0]
    duplicate = {**product, "sku": product["sku"].lower()}

    with pytest.raises(ValueError, match="Duplicate canonical catalog SKU"):
        validate_product_catalog([product, duplicate])
