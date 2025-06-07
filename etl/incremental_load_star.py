# etl_scripts/incremental_load_star.py

import psycopg2
from config.settings import DB_CONFIG

def incremental_load_star():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # ───────────────────────────────────────────────────────────
        # 1) DIM_DATE: new full_date values from exchange_rates
        # ───────────────────────────────────────────────────────────
        cur.execute("""
            INSERT INTO star.dim_date
              (full_date, year, quarter, month, day, day_of_week)
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
            ) AS src
            WHERE NOT EXISTS (
              SELECT 1
                FROM star.dim_date dst
               WHERE dst.full_date = src.d
            )
            ORDER BY d;
        """)
        conn.commit()

        # ───────────────────────────────────────────────────────────
        # 2) DIM_CATEGORY
        # ───────────────────────────────────────────────────────────
        cur.execute("""
            INSERT INTO star.dim_category
              (category_id, category_name)
            SELECT
              c.category_id,
              c.category_name
            FROM warehouse.categories c
            WHERE c.end_date = '9999-12-31'
              AND NOT EXISTS (
                SELECT 1
                  FROM star.dim_category dc
                 WHERE dc.category_id = c.category_id
              );
        """)
        conn.commit()

        # ───────────────────────────────────────────────────────────
        # 3) DIM_USER
        # ───────────────────────────────────────────────────────────
        cur.execute("""
            INSERT INTO star.dim_user
              (user_id, user_name)
            SELECT
              u.user_id,
              u.user_name
            FROM warehouse.users u
            WHERE u.end_date = '9999-12-31'
              AND NOT EXISTS (
                SELECT 1
                  FROM star.dim_user du
                 WHERE du.user_id = u.user_id
              );
        """)
        conn.commit()

        # ───────────────────────────────────────────────────────────
        # 4) DIM_PRODUCT
        # ───────────────────────────────────────────────────────────
        cur.execute("""
            INSERT INTO star.dim_product
              (product_id, product_name, category_sk)
            SELECT
              p.product_id,
              p.product_name,
              dc.category_sk
            FROM warehouse.products p
            JOIN warehouse.categories wc
              ON p.category_sk = wc.categories_sk
             AND wc.end_date = '9999-12-31'
            JOIN star.dim_category dc
              ON wc.category_id = dc.category_id
            WHERE p.end_date = '9999-12-31'
              AND NOT EXISTS (
                SELECT 1
                  FROM star.dim_product dp
                 WHERE dp.product_id = p.product_id
              );
        """)
        conn.commit()

        # ───────────────────────────────────────────────────────────
        # 5) DIM_LOCATION
        # ───────────────────────────────────────────────────────────
        cur.execute("""
            INSERT INTO star.dim_location
              (location_id, country, city)
            SELECT
              l.location_id,
              l.country,
              l.city
            FROM warehouse.locations l
            WHERE l.end_date = '9999-12-31'
              AND NOT EXISTS (
                SELECT 1
                  FROM star.dim_location dl
                 WHERE dl.location_id = l.location_id
              );
        """)
        conn.commit()

        # ───────────────────────────────────────────────────────────
        # 6) FACT_PRICING
        # ───────────────────────────────────────────────────────────
        # Insert only those date+product pairs not yet in fact_pricing
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
            FROM warehouse.exchange_rates er
            JOIN warehouse.products          pr
              ON er.product_sk = pr.products_sk
             AND pr.end_date   = '9999-12-31'
            JOIN star.dim_product           dp
              ON pr.product_id = dp.product_id
            JOIN star.dim_date              dd
              ON CAST(er.start_date AS DATE) = dd.full_date
            WHERE er.end_date = '9999-12-31'
              AND NOT EXISTS (
                SELECT 1
                  FROM star.fact_pricing fp
                  JOIN star.dim_date   d2 ON fp.date_sk    = d2.date_sk
                 WHERE d2.full_date    = CAST(er.start_date AS DATE)
                   AND fp.product_sk   = dp.product_sk
              );
        """)
        conn.commit()

        print("✔ Incremental load into star.schema completed.")
        cur.close()

    except Exception as e:
        print("ERROR during incremental load into star.schema:", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    incremental_load_star()
