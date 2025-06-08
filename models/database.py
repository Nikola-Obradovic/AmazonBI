# models/database.py

import os
import psycopg2
import pandas as pd
from config.settings import DB_CONFIG

MAX_VARCHAR = 255

def safe_trunc(s: str, length: int = MAX_VARCHAR):
    """Trim s to at most length characters (if it’s a string)."""
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= length else s[:length]

def create_database_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def clear_data(conn):
    """Remove all rows from all tables so we can start fresh."""
    cur = conn.cursor()
    # Truncate in dependency order:
    cur.execute("""
      TRUNCATE locations,
               reviews,
               products,
               users,
               categories
        RESTART IDENTITY
        CASCADE;
    """)
    conn.commit()
    cur.close()

def create_tables(conn):
    commands = (
        """
        CREATE TABLE IF NOT EXISTS categories (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(255) NOT NULL UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(255) PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            category_id INTEGER REFERENCES categories(category_id),
            discounted_price DECIMAL(10,2),
            actual_price DECIMAL(10,2) NOT NULL,
            discount_percentage DECIMAL(5,2),
            rating DECIMAL(3,2),
            rating_count INTEGER,
            about_product TEXT,
            product_link TEXT,
            currency VARCHAR(10)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR(255) PRIMARY KEY,
            user_name VARCHAR(255) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS reviews (
            review_id VARCHAR(255) PRIMARY KEY,
            product_id VARCHAR(255) REFERENCES products(product_id),
            user_id VARCHAR(255) REFERENCES users(user_id),
            review_title VARCHAR(255),
            review_content TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS locations (
            location_id SERIAL PRIMARY KEY,
            product_id VARCHAR(255) REFERENCES products(product_id),
            country VARCHAR(100),
            city VARCHAR(100)
        );
        """
    )
    cur = conn.cursor()
    for cmd in commands:
        cur.execute(cmd)
    conn.commit()
    cur.close()

def insert_data_from_csv(conn, csv_file_path):
    df = pd.read_csv(csv_file_path).where(pd.notnull(pd.read_csv(csv_file_path)), None)

    # only take the first category before any '|'
    df['main_category'] = df['category'].apply(
        lambda c: c.split('|')[0].strip() if c else None
    )

    cur = conn.cursor()

    # 1) categories
    for cat in df['main_category'].dropna().unique():
        cat = safe_trunc(cat)
        cur.execute("""
            INSERT INTO categories (category_name)
            VALUES (%s)
            ON CONFLICT (category_name) DO NOTHING
        """, (cat,))
    conn.commit()

    # 2) products
    def clean_money(x):
        if not x: return None
        s = str(x).replace('₹','').replace(',','').strip()
        return float(s) if s else None

    for _, row in df.iterrows():
        if not row['actual_price']:
            continue

        # lookup category_id
        if row['main_category']:
            cur.execute("SELECT category_id FROM categories WHERE category_name = %s",
                        (safe_trunc(row['main_category']),))
            cat_id = cur.fetchone()[0]
        else:
            cat_id = None

        dp = clean_money(row['discounted_price'])
        ap = clean_money(row['actual_price'])

        disc_pct = None
        if row['discount_percentage']:
            pct = str(row['discount_percentage']).replace('%','').strip()
            disc_pct = float(pct) if pct else None

        try:
            rating = float(row['rating']) if row['rating'] else None
        except:
            rating = None

        rc = None
        if row['rating_count']:
            tmp = str(row['rating_count']).replace(',','').strip()
            rc = int(tmp) if tmp.isdigit() else None

        cur.execute("""
            INSERT INTO products (
                product_id, product_name, category_id,
                discounted_price, actual_price,
                discount_percentage, rating, rating_count,
                about_product, product_link, currency
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (product_id) DO NOTHING
        """, (
            safe_trunc(row['product_id'], 255),
            safe_trunc(row['product_name'], 255),
            cat_id,
            dp, ap,
            disc_pct,
            rating, rc,
            row['about_product'],   # TEXT, no truncation needed
            row['product_link'],    # TEXT
            safe_trunc(row['currency'], 10)
        ))
    conn.commit()

    # 3) users
    for _, r in df[['user_id','user_name']].drop_duplicates().iterrows():
        if r['user_id'] and r['user_name']:
            cur.execute("""
                INSERT INTO users (user_id, user_name)
                VALUES (%s,%s)
                ON CONFLICT (user_id) DO NOTHING
            """, (
                safe_trunc(r['user_id'], 255),
                safe_trunc(r['user_name'], 255),
            ))
    conn.commit()

    # 4) reviews
    for _, r in df[['review_id','product_id','user_id','review_title','review_content']].drop_duplicates().iterrows():
        if r['review_id'] and r['product_id'] and r['user_id']:
            cur.execute("""
                INSERT INTO reviews
                  (review_id, product_id, user_id, review_title, review_content)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (review_id) DO NOTHING
            """, (
                safe_trunc(r['review_id'],255),
                safe_trunc(r['product_id'],255),
                safe_trunc(r['user_id'],255),
                safe_trunc(r['review_title'],255),
                r['review_content']  # TEXT
            ))
    conn.commit()

    # 5) locations
    for _, r in df[['product_id','country','city']].drop_duplicates().iterrows():
        if not r['product_id']:
            continue
        cur.execute("SELECT 1 FROM products WHERE product_id = %s", (r['product_id'],))
        if cur.fetchone():
            cur.execute("""
                INSERT INTO locations (product_id, country, city)
                VALUES (%s,%s,%s)
            """, (
                safe_trunc(r['product_id'],255),
                safe_trunc(r['country'],100),
                safe_trunc(r['city'],100),
            ))
    conn.commit()

    cur.close()
    print("Data inserted successfully!")

def main():
    THIS_DIR = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(THIS_DIR, 'amazon_products_cleaned.csv')

    conn = create_database_connection()
    if conn:
        # 0) wipe out _all_ existing rows:
        clear_data(conn)

        # 1) ensure tables exist
        create_tables(conn)

        # 2) load fresh
        insert_data_from_csv(conn, csv_path)

        conn.close()

if __name__ == "__main__":
    main()