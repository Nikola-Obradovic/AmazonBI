# models/database.py

import os
import psycopg2
import pandas as pd
from config.settings import DB_CONFIG  # Assumes config/__init__.py exists

def create_database_connection():
    """Create and return a psycopg2 database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def create_tables(conn):
    """Create the database tables if they don't already exist."""
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

    try:
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()

def insert_data_from_csv(conn, csv_file_path):
    """
    Read the CSV at `csv_file_path`, clean it up, and insert all data into the database.
    1. We only insert a product if `actual_price` is present (because actual_price is NOT NULL).
    2. When inserting locations, we skip any row whose product_id is not in `products`.
    """
    try:
        # 1) Read CSV file into a DataFrame
        df = pd.read_csv(csv_file_path)

        # 2) Replace NaN → None
        df = df.where(pd.notnull(df), None)

        # 2a) Derive a 'main_category' column (take only the first part before '|')
        def extract_first_category(cat):
            if not cat:
                return None
            first = cat.split('|')[0].strip()
            return first if first else None

        df['main_category'] = df['category'].apply(extract_first_category)

        cur = conn.cursor()

        # -----------------------
        # 3) Insert categories (using 'main_category')
        # -----------------------
        unique_cats = df['main_category'].dropna().unique()
        for cat in unique_cats:
            try:
                cur.execute(
                    """
                    INSERT INTO categories (category_name)
                    VALUES (%s)
                    ON CONFLICT (category_name) DO NOTHING
                    """,
                    (cat,)
                )
            except Exception as e:
                print(f"Error inserting category '{cat}': {e}")
                conn.rollback()
        conn.commit()

        # -----------------------
        # 4) Insert products
        # -----------------------
        for _, row in df.iterrows():
            # 4a) Clean & parse the numeric/text fields
            #    Skip this entire product if actual_price is missing or empty,
            #    because actual_price is defined as NOT NULL.
            if not row['actual_price']:
                # We cannot insert a product without actual_price
                # (that would violate the table definition).
                print(f"Skipping product '{row['product_id']}' because actual_price is missing.")
                continue

            # 4b) Look up category_id for row['main_category']
            if row['main_category']:
                cur.execute(
                    "SELECT category_id FROM categories WHERE category_name = %s",
                    (row['main_category'],)
                )
                cat_row = cur.fetchone()
                category_id = cat_row[0] if cat_row else None
            else:
                category_id = None

            # 4c) Parse discounted_price / actual_price (strip ₹ and commas)
            dp = None
            if row['discounted_price']:
                dp_str = str(row['discounted_price']).replace('₹', '').replace(',', '').strip()
                dp = float(dp_str) if dp_str else None

            ap_str = str(row['actual_price']).replace('₹', '').replace(',', '').strip()
            actual_price_val = float(ap_str) if ap_str else None
            # (We already checked that row['actual_price'] is not None)

            # 4d) Parse discount_percentage (strip '%')
            disc_pct = None
            if row['discount_percentage']:
                pct_str = str(row['discount_percentage']).replace('%', '').strip()
                disc_pct = float(pct_str) if pct_str else None

            # 4e) Parse rating
            rating_val = None
            if row['rating']:
                try:
                    rating_val = float(row['rating'])
                except:
                    rating_val = None

            # 4f) Parse rating_count (strip commas)
            rating_cnt = None
            if row['rating_count']:
                rc_str = str(row['rating_count']).replace(',', '').strip()
                rating_cnt = int(rc_str) if rc_str.isdigit() else None

            # 4g) Now insert into products
            try:
                cur.execute(
                    """
                    INSERT INTO products (
                        product_id, product_name, category_id,
                        discounted_price, actual_price,
                        discount_percentage, rating, rating_count,
                        about_product, product_link, currency
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (product_id) DO NOTHING
                    """,
                    (
                        row['product_id'],
                        row['product_name'],
                        category_id,
                        dp,
                        actual_price_val,
                        disc_pct,
                        rating_val,
                        rating_cnt,
                        row['about_product'],
                        row['product_link'],
                        row['currency']
                    )
                )
            except Exception as e:
                print(f"Error inserting product '{row['product_id']}': {e}")
                conn.rollback()

        conn.commit()

        # -----------------------
        # 5) Insert users (de-duplicate by user_id)
        # -----------------------
        user_df = df[['user_id', 'user_name']].drop_duplicates()
        for _, row in user_df.iterrows():
            if row['user_id'] and row['user_name']:
                try:
                    cur.execute(
                        """
                        INSERT INTO users (user_id, user_name)
                        VALUES (%s,%s)
                        ON CONFLICT (user_id) DO NOTHING
                        """,
                        (row['user_id'], row['user_name'])
                    )
                except Exception as e:
                    print(f"Error inserting user '{row['user_id']}': {e}")
                    conn.rollback()
        conn.commit()

        # -----------------------
        # 6) Insert reviews (de-duplicate by review_id)
        # -----------------------
        review_df = df[['review_id', 'product_id', 'user_id', 'review_title', 'review_content']].drop_duplicates()
        for _, row in review_df.iterrows():
            if row['review_id'] and row['product_id'] and row['user_id']:
                try:
                    cur.execute(
                        """
                        INSERT INTO reviews
                            (review_id, product_id, user_id, review_title, review_content)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (review_id) DO NOTHING
                        """,
                        (
                            row['review_id'],
                            row['product_id'],
                            row['user_id'],
                            row['review_title'],
                            row['review_content']
                        )
                    )
                except Exception as e:
                    print(f"Error inserting review '{row['review_id']}': {e}")
                    conn.rollback()
        conn.commit()

        # -----------------------
        # 7) Insert locations (only if product_id exists in products)
        # -----------------------
        loc_df = df[['product_id', 'country', 'city']].drop_duplicates()
        for _, row in loc_df.iterrows():
            pid = row['product_id']
            if not pid:
                continue  # skip if no product_id

            # Before inserting, check if this product_id is actually in "products"
            cur.execute(
                "SELECT 1 FROM products WHERE product_id = %s",
                (pid,)
            )
            exists = cur.fetchone()

            if not exists:
                # If the product wasn't inserted (e.g., skipped earlier because actual_price was missing),
                # then we skip this location row to avoid foreign key violation.
                print(f"Skipping location insertion for product '{pid}' because it does not exist in products.")
                continue

            # Now, safe to insert location for an existing product
            try:
                cur.execute(
                    """
                    INSERT INTO locations (product_id, country, city)
                    VALUES (%s,%s,%s)
                    """,
                    (
                        pid,
                        row['country'],
                        row['city']
                    )
                )
            except Exception as e:
                print(f"Error inserting location for product '{pid}': {e}")
                conn.rollback()

        conn.commit()
        cur.close()
        print("Data inserted successfully!")

    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()

def main():
    # ───────────────────────────────────────────────────────────
    # Assume this file lives at <project_root>/models/database.py
    THIS_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(THIS_FILE_DIR, 'amazon_products_cleaned.csv')

    conn = create_database_connection()
    if conn is not None:
        create_tables(conn)
        insert_data_from_csv(conn, csv_file_path)
        conn.close()

if __name__ == "__main__":
    main()
