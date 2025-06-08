# etl_scripts/add_mock_data.py

import psycopg2
from config.settings import DB_CONFIG
from datetime import datetime

MAX_VARCHAR = 255

def safe_trunc(s: str, length: int = MAX_VARCHAR):
    """Trim s to at most `length` characters."""
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= length else s[:length]

def clean_money(x):
    """Strip currency symbols/commas and convert to float."""
    if x is None:
        return None
    s = str(x).replace('₹','').replace(',','').strip()
    return float(s) if s else None

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # --------------------------------------------------------------------------
    # 1) Insert mock categories
    # --------------------------------------------------------------------------
    mock_cats = ["MockCategoryA", "MockCategoryB"]
    for cat in mock_cats:
        cur.execute("""
            INSERT INTO categories (category_name)
            VALUES (%s)
            ON CONFLICT (category_name) DO NOTHING;
        """, (safe_trunc(cat),))
    conn.commit()

    # Map category names → IDs
    cur.execute("""
        SELECT category_id, category_name
          FROM categories
         WHERE category_name = ANY(%s);
    """, (mock_cats,))
    cat_map = {name: cid for cid, name in cur.fetchall()}

    # --------------------------------------------------------------------------
    # 2) Insert mock products
    # --------------------------------------------------------------------------
    mock_products = [
        {
            "product_id": "MOCK-P1",
            "product_name": "Mock Product One",
            "category": "MockCategoryA",
            "discounted_price": "499.00",
            "actual_price": "599.00",
            "discount_percentage": "16.7",
            "rating": "4.2",
            "rating_count": "10",
            "about_product": "This is the first mock product.",
            "product_link": "https://example.com/mock1",
            "currency": "USD"
        },
        {
            "product_id": "MOCK-P2",
            "product_name": "Mock Product Two",
            "category": "MockCategoryA",
            "discounted_price": "299.00",
            "actual_price": "399.00",
            "discount_percentage": "25.1",
            "rating": "3.8",
            "rating_count": "7",
            "about_product": "Second mock product details.",
            "product_link": "https://example.com/mock2",
            "currency": "USD"
        },
        {
            "product_id": "MOCK-P3",
            "product_name": "Mock Product Three",
            "category": "MockCategoryB",
            "discounted_price": None,
            "actual_price": "129.99",
            "discount_percentage": None,
            "rating": "4.9",
            "rating_count": "25",
            "about_product": "Third mock product info.",
            "product_link": "https://example.com/mock3",
            "currency": "USD"
        },
    ]
    for p in mock_products:
        cat_id = cat_map.get(p["category"])
        cur.execute("""
            INSERT INTO products (
                product_id, product_name, category_id,
                discounted_price, actual_price,
                discount_percentage, rating, rating_count,
                about_product, product_link, currency
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (product_id) DO NOTHING;
        """, (
            safe_trunc(p["product_id"],255),
            safe_trunc(p["product_name"],255),
            cat_id,
            clean_money(p["discounted_price"]),
            clean_money(p["actual_price"]),
            float(p["discount_percentage"]) if p["discount_percentage"] else None,
            float(p["rating"]) if p["rating"] else None,
            int(p["rating_count"]) if p["rating_count"] else None,
            p["about_product"],
            p["product_link"],
            safe_trunc(p["currency"],10)
        ))
    conn.commit()

    # --------------------------------------------------------------------------
    # 3) Insert mock users
    # --------------------------------------------------------------------------
    mock_users = [
        {"user_id": "MOCK-U1", "user_name": "Alice Mock"},
        {"user_id": "MOCK-U2", "user_name": "Bob Mockson"},
        {"user_id": "MOCK-U3", "user_name": "Carol Mockowitz"},
    ]
    for u in mock_users:
        cur.execute("""
            INSERT INTO users (user_id, user_name)
            VALUES (%s,%s)
            ON CONFLICT (user_id) DO NOTHING;
        """, (
            safe_trunc(u["user_id"],255),
            safe_trunc(u["user_name"],255)
        ))
    conn.commit()

    # --------------------------------------------------------------------------
    # 4) Insert mock reviews
    # --------------------------------------------------------------------------
    mock_reviews = [
        {"review_id": "MOCK-R1", "product_id": "MOCK-P1", "user_id": "MOCK-U1",
         "review_title": "Loved it!", "review_content": "This mock product is awesome."},
        {"review_id": "MOCK-R2", "product_id": "MOCK-P2", "user_id": "MOCK-U2",
         "review_title": "Pretty good", "review_content": "Satisfied with this mock."},
        {"review_id": "MOCK-R3", "product_id": "MOCK-P2", "user_id": "MOCK-U3",
         "review_title": "Could improve", "review_content": "Not bad, but could be better."},
        {"review_id": "MOCK-R4", "product_id": "MOCK-P3", "user_id": "MOCK-U1",
         "review_title": "Excellent", "review_content": "Exceeded my mock expectations."},
    ]
    for r in mock_reviews:
        cur.execute("""
            INSERT INTO reviews
              (review_id, product_id, user_id, review_title, review_content)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (review_id) DO NOTHING;
        """, (
            safe_trunc(r["review_id"],255),
            safe_trunc(r["product_id"],255),
            safe_trunc(r["user_id"],255),
            safe_trunc(r["review_title"],255),
            r["review_content"]
        ))
    conn.commit()

    # --------------------------------------------------------------------------
    # 5) Insert mock locations
    # --------------------------------------------------------------------------
    mock_locs = [
        {"product_id": "MOCK-P1", "country": "USA", "city": "New York"},
        {"product_id": "MOCK-P2", "country": "Canada", "city": "Toronto"},
        {"product_id": "MOCK-P3", "country": "UK", "city": "London"},
    ]
    for l in mock_locs:
        cur.execute("SELECT 1 FROM products WHERE product_id = %s", (l["product_id"],))
        if cur.fetchone():
            cur.execute("""
                INSERT INTO locations (product_id, country, city)
                VALUES (%s,%s,%s);
            """, (
                safe_trunc(l["product_id"],255),
                safe_trunc(l["country"],100),
                safe_trunc(l["city"],100)
            ))
    conn.commit()

    # --------------------------------------------------------------------------
    # 6) Update currency on exactly these four real products → EUR
    # --------------------------------------------------------------------------
    products_to_update = [
        "B08HDJ86NZ",
        "B08CF3B7N1",
        "B09L1TF5P6",
        "B09KLMVZ3B"
    ]
    for pid in products_to_update:
        cur.execute("""
            UPDATE products
               SET currency = %s
             WHERE product_id = %s
        """, (safe_trunc("EUR", 10), pid))
    conn.commit()

    # --------------------------------------------------------------------------
    # 7) Delete exactly these three real products (and their reviews/locations)
    # --------------------------------------------------------------------------
    to_delete = [
        "B07JW9H4J1",
        "B098NS6PVG",
        "B096MSW6CT"
    ]
    # remove dependent reviews
    cur.execute("""
        DELETE FROM reviews
         WHERE product_id = ANY(%s)
    """, (to_delete,))
    # remove dependent locations
    cur.execute("""
        DELETE FROM locations
         WHERE product_id = ANY(%s)
    """, (to_delete,))
    # now delete the products themselves
    cur.execute("""
        DELETE FROM products
         WHERE product_id = ANY(%s)
    """, (to_delete,))
    conn.commit()

    cur.close()
    conn.close()
    print("✅ Mock data inserted, currencies updated, and specified products deleted.")

if __name__ == "__main__":
    main()
