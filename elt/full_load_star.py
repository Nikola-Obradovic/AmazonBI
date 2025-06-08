# etl_scripts/full_load_star.py

import psycopg2
from config.settings import DB_CONFIG

def full_load_star():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1) Truncate all star tables
        for tbl in (
            "star.fact_pricing",
            "star.dim_date",
            "star.dim_product",
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
                FROM warehouse.exchange_rates
               WHERE end_date = '9999-12-31'
            ) AS dates
            ORDER BY d;
        """)
        conn.commit()

        # 3) dim_category
        cur.execute("""
            INSERT INTO star.dim_category (category_id, category_name)
            SELECT DISTINCT
              c.category_id,
              c.category_name
            FROM warehouse.categories c
            WHERE c.end_date = '9999-12-31';
        """)
        conn.commit()

        # 4) dim_product  — only product_id + name now
        cur.execute("""
            INSERT INTO star.dim_product (product_id, product_name)
            SELECT DISTINCT
              p.product_id,
              p.product_name
            FROM warehouse.products p
            WHERE p.end_date = '9999-12-31';
        """)
        conn.commit()

        # 5) dim_location
        cur.execute("""
            INSERT INTO star.dim_location (location_id, country, city)
            SELECT DISTINCT
              l.location_id,
              l.country,
              l.city
            FROM warehouse.locations l
            WHERE l.end_date = '9999-12-31';
        """)
        conn.commit()

        # 6) fact_pricing
        cur.execute("""
            INSERT INTO star.fact_pricing
              (date_sk,
               product_sk,
               category_sk,
               location_sk,
               actual_price,
               discounted_price,
               discount_percentage,
               currency,
               rate_to_base)
            SELECT
              dd.date_sk,
              dp.product_sk,
              dc.category_sk,
              dl.location_sk,
              pr.actual_price,
              pr.discounted_price,
              pr.discount_percentage,
              pr.currency,
              er.rate_to_base
            FROM warehouse.exchange_rates er

            -- link to product
            JOIN warehouse.products pr
              ON er.product_sk = pr.products_sk
             AND pr.end_date   = '9999-12-31'

            -- date lookup
            JOIN star.dim_date dd
              ON CAST(er.start_date AS DATE) = dd.full_date

            -- product SK
            JOIN star.dim_product dp
              ON pr.product_id = dp.product_id

            -- category SK from dim_category via natural key
            JOIN warehouse.categories wc
              ON pr.category_sk = wc.categories_sk
             AND wc.end_date = '9999-12-31'
            JOIN star.dim_category dc
              ON wc.category_id = dc.category_id

            -- location SK
            JOIN warehouse.locations wl
              ON pr.products_sk = wl.product_sk
             AND wl.end_date   = '9999-12-31'
            JOIN star.dim_location dl
              ON wl.location_id = dl.location_id

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
