import psycopg2
from config.settings import DB_CONFIG

def incremental_load_star():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1) DIM_DATE: add any new dates
        cur.execute("""
            INSERT INTO star.dim_date (full_date, year, quarter, month, day, day_of_week)
            SELECT d::date,
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
              SELECT 1 FROM star.dim_date dst
               WHERE dst.full_date = src.d
            )
            ORDER BY d;
        """)
        conn.commit()

        # 2) DIM_CATEGORY: new categories
        cur.execute("""
            INSERT INTO star.dim_category (category_id, category_name)
            SELECT c.category_id, c.category_name
              FROM warehouse.categories c
             WHERE c.end_date = '9999-12-31'
               AND NOT EXISTS (
                 SELECT 1 FROM star.dim_category dc
                  WHERE dc.category_id = c.category_id
               );
        """)
        conn.commit()

        # 3) DIM_PRODUCT: new products
        cur.execute("""
            INSERT INTO star.dim_product (product_id, product_name)
            SELECT p.product_id, p.product_name
              FROM warehouse.products p
             WHERE p.end_date = '9999-12-31'
               AND NOT EXISTS (
                 SELECT 1 FROM star.dim_product dp
                  WHERE dp.product_id = p.product_id
               );
        """)
        conn.commit()

        # 4) DIM_LOCATION: new locations
        cur.execute("""
            INSERT INTO star.dim_location (location_id, country, city)
            SELECT l.location_id, l.country, l.city
              FROM warehouse.locations l
             WHERE l.end_date = '9999-12-31'
               AND NOT EXISTS (
                 SELECT 1 FROM star.dim_location dl
                  WHERE dl.location_id = l.location_id
               );
        """)
        conn.commit()

        # 5) FACT_PRICING: only new (date,product,location),
        #    now pulling category_sk via warehouse.categories → star.dim_category
        cur.execute("""
            INSERT INTO star.fact_pricing
              (date_sk, product_sk, category_sk, location_sk,
               actual_price, discounted_price, discount_percentage,
               currency, rate_to_base)
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
            JOIN warehouse.products      pr
              ON er.product_sk = pr.products_sk
             AND pr.end_date   = '9999-12-31'

            -- map to star.dim_date
            JOIN star.dim_date          dd
              ON CAST(er.start_date AS DATE) = dd.full_date

            -- map to star.dim_product
            JOIN star.dim_product       dp
              ON pr.product_id = dp.product_id

            -- find the right category_sk:
            JOIN warehouse.categories   wc
              ON pr.category_sk = wc.categories_sk
             AND wc.end_date   = '9999-12-31'
            JOIN star.dim_category      dc
              ON wc.category_id = dc.category_id

            -- map to star.dim_location
            JOIN warehouse.locations    wl
              ON pr.products_sk = wl.product_sk
             AND wl.end_date    = '9999-12-31'
            JOIN star.dim_location      dl
              ON wl.location_id = dl.location_id

            WHERE er.end_date = '9999-12-31'
              AND NOT EXISTS (
                SELECT 1
                  FROM star.fact_pricing fp
                 WHERE fp.date_sk     = dd.date_sk
                   AND fp.product_sk  = dp.product_sk
                   AND fp.location_sk = dl.location_sk
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
