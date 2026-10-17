import hashlib
from numbers import Real


def validate_product_catalog(products: list[dict]) -> list[dict]:
    """Validate and normalize records returned by the PIM catalog endpoint."""
    if not isinstance(products, list) or not products:
        raise ValueError("Canonical product catalog must contain at least one product.")

    normalized_skus = set()
    validated_products = []
    for product in products:
        if not isinstance(product, dict):
            raise ValueError("Every catalog product must be an object.")
        sku = product.get("sku")
        source_description = product.get("description")
        category = product.get("category")
        price = product.get("price")
        if not all(
            isinstance(value, str) and value.strip()
            for value in (sku, source_description, category)
        ):
            raise ValueError(
                "Catalog SKU, description, and category must be non-blank."
            )
        sku_key = sku.strip().upper()
        if sku_key in normalized_skus:
            raise ValueError(f"Duplicate canonical catalog SKU: {sku}")
        normalized_skus.add(sku_key)
        if not isinstance(price, Real) or isinstance(price, bool) or price < 0:
            raise ValueError(f"Invalid price for catalog SKU {sku}.")

        validated_products.append(
            {
                "sku": sku.strip(),
                "source_description": source_description,
                "canonical_description": source_description.strip(),
                "category": category.strip(),
                "price": float(price),
            }
        )
    return validated_products


def product_catalog_to_dimension_records(products: list[dict]) -> list[dict]:
    """Shape validated PIM products for Parquet extraction and the Silver dimension."""
    records = []
    for product in validate_product_catalog(products):
        product_hash = hashlib.sha256(
            repr(
                (
                    product["sku"],
                    product["source_description"],
                    product["canonical_description"],
                    product["category"],
                    product["price"],
                )
            ).encode("utf-8")
        ).hexdigest()
        records.append(
            {
                "sku": product["sku"],
                "source_description": product["source_description"],
                "canonical_description": product["canonical_description"],
                "category": product["category"],
                "price": product["price"],
                "product_hash": product_hash,
            }
        )
    return records
