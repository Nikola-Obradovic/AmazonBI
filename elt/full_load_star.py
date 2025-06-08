# etl_scripts/full_load_star.py

import psycopg2
from config.settings import DB_CONFIG

def full_load_star():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1) Truncate all star tables (no fact_review)
        for tbl in (
            "star.fact_pricing",
            "star.dim_date",
            "star.dim_product",
            "star.dim_user",
            "star.dim_category",
            "star.dim_location",
        ):
            cur.execute(f"TRUNCATE {tbl} CASCADE;")
        conn.commit()

        # 2) dim_date
        cur.execute("""
            INSERT INTO star.dim_date (full_date, year, quarter, month, day, day_of_week)
            SELECT
              d::date,
              EXTRACT(YEAR   FROM d),
              EXTRACT(QUARTER FROM d),
              EXTRACT(MONTH  FROM d),
              EXTRACT(DAY    FROM d),
              EXTRACT(DOW    FROM d)
            FROM (
              SELECT DISTINCT CAST(start_date AS DATE) AS d
                FROM warehouse.reviews    WHERE end_date = '9999-12-31'
              UNION
              SELECT DISTINCT CAST(start_date AS DATE) AS d
                FROM warehouse.exchange_rates WHERE end_date = '9999-12-31'
            ) AS dt
            ORDER BY d;
        """)
        conn.commit()

        # 3) dim_category
        cur.execute("""
            INSERT INTO star.dim_category (category_id, category_name)
            SELECT DISTINCT category_id, category_name
              FROM warehouse.categories
             WHERE end_date = '9999-12-31';
        """)
        conn.commit()

        # 4) dim_user
        cur.execute("""
            INSERT INTO star.dim_user (user_id, user_name)
            SELECT DISTINCT user_id, user_name
              FROM warehouse.users
             WHERE end_date = '9999-12-31';
        """)
        conn.commit()

        # 5) dim_product
        cur.execute("""
            INSERT INTO star.dim_product (product_id, product_name, category_sk)
            SELECT DISTINCT
              p.product_id,
              p.product_name,
              dc.category_sk
            FROM warehouse.products AS p
            JOIN warehouse.categories AS wc
              ON p.category_sk = wc.categories_sk
             AND wc.end_date = '9999-12-31'
            JOIN star.dim_category AS dc
              ON wc.category_id = dc.category_id
            WHERE p.end_date = '9999-12-31';
        """)
        conn.commit()

        # 6) dim_location
        cur.execute("""
            INSERT INTO star.dim_location (location_id, country, city)
            SELECT DISTINCT location_id, country, city
              FROM warehouse.locations
             WHERE end_date = '9999-12-31';
        """)
        conn.commit()

        # 7) fact_pricing
        cur.execute("""
            INSERT INTO star.fact_pricing
              (date_sk, product_sk,
               actual_price, discounted_price, discount_percentage,
               currency, rate_to_base)
            SELECT
              dd.date_sk,
              dp.product_sk,
              pr.actual_price,
              pr.discounted_price,
              pr.discount_percentage,
              pr.currency,
              er.rate_to_base
            FROM warehouse.exchange_rates AS er
            JOIN warehouse.products         AS pr
              ON er.product_sk = pr.products_sk
             AND pr.end_date   = '9999-12-31'
            JOIN star.dim_product          AS dp
              ON pr.product_id = dp.product_id
            JOIN star.dim_date             AS dd
              ON CAST(er.start_date AS DATE) = dd.full_date
            WHERE er.end_date = '9999-12-31';
        """)
        conn.commit()

        print("★ Full load into star schema completed successfully.")
        cur.close()

    except Exception as e:
        print("ERROR during full load into star schema:", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    full_load_star()
