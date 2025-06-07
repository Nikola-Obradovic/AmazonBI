# etl_scripts/full_load_warehouse.py

import psycopg2
from datetime import datetime
from config.settings import DB_CONFIG

def get_next_etl_id(cur):
    """
    Ensure a sequence 'warehouse.etl_seq' exists and return the next value.
    This sequence is used for both insert_id and update_id across ALL warehouse tables.
    """
    # 1) Create the sequence if it doesn’t already exist.
    cur.execute("""
        CREATE SEQUENCE IF NOT EXISTS warehouse.etl_seq START WITH 1;
    """)

    # 2) Find the greatest insert_id or update_id currently in any warehouse table.
    cur.execute("""
        SELECT GREATEST(
          COALESCE((SELECT MAX(insert_id) FROM warehouse.categories), 0),
          COALESCE((SELECT MAX(update_id) FROM warehouse.categories), 0),

          COALESCE((SELECT MAX(insert_id) FROM warehouse.users), 0),
          COALESCE((SELECT MAX(update_id) FROM warehouse.users), 0),

          COALESCE((SELECT MAX(insert_id) FROM warehouse.products), 0),
          COALESCE((SELECT MAX(update_id) FROM warehouse.products), 0),

          COALESCE((SELECT MAX(insert_id) FROM warehouse.reviews), 0),
          COALESCE((SELECT MAX(update_id) FROM warehouse.reviews), 0),

          COALESCE((SELECT MAX(insert_id) FROM warehouse.locations), 0),
          COALESCE((SELECT MAX(update_id) FROM warehouse.locations), 0),

          COALESCE((SELECT MAX(insert_id) FROM warehouse.exchange_rates), 0),
          COALESCE((SELECT MAX(update_id) FROM warehouse.exchange_rates), 0)
        );
    """)
    max_id = cur.fetchone()[0] or 0
    next_id = max_id + 1

    # 3) Bump the sequence so that nextval(…) will return exactly next_id.
    cur.execute("SELECT setval('warehouse.etl_seq', %s, false);", (next_id,))
    cur.execute("SELECT nextval('warehouse.etl_seq');")
    return cur.fetchone()[0]


def full_load_warehouse():
    """
    Perform a full load from:
      - public.categories, public.products, public.users, public.reviews, public.locations
      - staging.exchange_rates_raw
    into the corresponding warehouse.* tables, according to the updated schema with surrogate keys.

    For this load:
      * We truncate all warehouse tables first.
      * We call get_next_etl_id() once at the top to get a new “run-level” ETL ID:
          - That becomes insert_id for every row inserted in this run.
          - update_id remains NULL (because we’re only inserting the first version).
      * source_id = 1 for rows from public.*
      * source_id = 2 for rows from staging.*
      * start_date = load_timestamp (the same for all rows)
      * end_date defaults to '9999-12-31' via the table definitions
    """

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1) Capture a single timestamp for start_date on all inserts
        load_ts = datetime.utcnow()

        # 2) Compute a brand-new ETL ID for this entire run
        #    (this will be used as insert_id for all newly loaded rows)
        run_etl_id = get_next_etl_id(cur)

        # ------------------------------------------------------------------------------
        # 3) Truncate all warehouse tables in dependency order
        #    (exchange_rates → locations → reviews → products → users → categories)
        # ------------------------------------------------------------------------------
        cur.execute("TRUNCATE warehouse.exchange_rates CASCADE;")
        cur.execute("TRUNCATE warehouse.locations      CASCADE;")
        cur.execute("TRUNCATE warehouse.reviews        CASCADE;")
        cur.execute("TRUNCATE warehouse.products       CASCADE;")
        cur.execute("TRUNCATE warehouse.users          CASCADE;")
        cur.execute("TRUNCATE warehouse.categories     CASCADE;")
        conn.commit()

        # ------------------------------------------------------------------------------
        # 4) Load public.categories → warehouse.categories
        # ------------------------------------------------------------------------------
        cur.execute(
            """
            INSERT INTO warehouse.categories
              (category_id, category_name,
               start_date, source_id, insert_id, update_id)
            SELECT
              c.category_id,
              c.category_name,
              %s        AS start_date,
              1         AS source_id,
              %s        AS insert_id,
              NULL      AS update_id
            FROM public.categories c;
            """,
            (load_ts, run_etl_id)
        )

        # ------------------------------------------------------------------------------
        # 5) Load public.users → warehouse.users
        # ------------------------------------------------------------------------------
        cur.execute(
            """
            INSERT INTO warehouse.users
              (user_id, user_name,
               start_date, source_id, insert_id, update_id)
            SELECT
              u.user_id,
              u.user_name,
              %s        AS start_date,
              1         AS source_id,
              %s        AS insert_id,
              NULL      AS update_id
            FROM public.users u;
            """,
            (load_ts, run_etl_id)
        )

        # ------------------------------------------------------------------------------
        # 6) Load public.products → warehouse.products
        #    (join to warehouse.categories to get category_sk)
        # ------------------------------------------------------------------------------
        cur.execute(
            """
            INSERT INTO warehouse.products
              (product_id, product_name, category_sk,
               discounted_price, actual_price, discount_percentage,
               rating, rating_count, about_product, product_link, currency,
               start_date, source_id, insert_id, update_id)
            SELECT
              p.product_id,
              p.product_name,
              wc.categories_sk       AS category_sk,
              p.discounted_price,
              p.actual_price,
              p.discount_percentage,
              p.rating,
              p.rating_count,
              p.about_product,
              p.product_link,
              p.currency,
              %s        AS start_date,
              1         AS source_id,
              %s        AS insert_id,
              NULL      AS update_id
            FROM public.products p
            JOIN warehouse.categories wc
              ON p.category_id = wc.category_id
             AND wc.end_date = '9999-12-31';
            """,
            (load_ts, run_etl_id)
        )

        # ------------------------------------------------------------------------------
        # 7) Load public.reviews → warehouse.reviews
        #    (join to warehouse.products and warehouse.users to get surrogate keys)
        # ------------------------------------------------------------------------------
        cur.execute(
            """
            INSERT INTO warehouse.reviews
              (review_id, product_sk, user_sk, review_title, review_content,
               start_date, source_id, insert_id, update_id)
            SELECT
              r.review_id,
              wp.products_sk       AS product_sk,
              wu.users_sk          AS user_sk,
              r.review_title,
              r.review_content,
              %s        AS start_date,
              1         AS source_id,
              %s        AS insert_id,
              NULL      AS update_id
            FROM public.reviews r
            JOIN warehouse.products wp
              ON r.product_id = wp.product_id
             AND wp.end_date = '9999-12-31'
            JOIN warehouse.users wu
              ON r.user_id = wu.user_id
             AND wu.end_date = '9999-12-31';
            """,
            (load_ts, run_etl_id)
        )

        # ------------------------------------------------------------------------------
        # 8) Load public.locations → warehouse.locations
        #    (join to warehouse.products to get product_sk)
        # ------------------------------------------------------------------------------
        cur.execute(
            """
            INSERT INTO warehouse.locations
              (location_id, product_sk, country, city,
               start_date, source_id, insert_id, update_id)
            SELECT
              l.location_id,
              wp.products_sk       AS product_sk,
              l.country,
              l.city,
              %s        AS start_date,
              1         AS source_id,
              %s        AS insert_id,
              NULL      AS update_id
            FROM public.locations l
            JOIN warehouse.products wp
              ON l.product_id = wp.product_id
             AND wp.end_date = '9999-12-31';
            """,
            (load_ts, run_etl_id)
        )

        conn.commit()

        # ------------------------------------------------------------------------------
        # 9) Reset the surrogate‐key sequences for all warehouse tables
        #    so that future inserts get the correct next SK
        # ------------------------------------------------------------------------------
        cur.execute(
            """
            SELECT setval(
                     pg_get_serial_sequence('warehouse.categories','categories_sk'),
                     COALESCE((SELECT MAX(categories_sk) FROM warehouse.categories), 1),
                     true
                   );
            """
        )
        cur.execute(
            """
            SELECT setval(
                     pg_get_serial_sequence('warehouse.users','users_sk'),
                     COALESCE((SELECT MAX(users_sk) FROM warehouse.users), 1),
                     true
                   );
            """
        )
        cur.execute(
            """
            SELECT setval(
                     pg_get_serial_sequence('warehouse.products','products_sk'),
                     COALESCE((SELECT MAX(products_sk) FROM warehouse.products), 1),
                     true
                   );
            """
        )
        cur.execute(
            """
            SELECT setval(
                     pg_get_serial_sequence('warehouse.reviews','reviews_sk'),
                     COALESCE((SELECT MAX(reviews_sk) FROM warehouse.reviews), 1),
                     true
                   );
            """
        )
        cur.execute(
            """
            SELECT setval(
                     pg_get_serial_sequence('warehouse.locations','locations_sk'),
                     COALESCE((SELECT MAX(locations_sk) FROM warehouse.locations), 1),
                     true
                   );
            """
        )
        conn.commit()

        # ------------------------------------------------------------------------------
        # 10) Load staging.exchange_rates_raw → warehouse.exchange_rates
        #     (join to warehouse.products to get product_sk)
        # ------------------------------------------------------------------------------
        cur.execute(
            """
            INSERT INTO warehouse.exchange_rates
              (product_sk, fetched_at, rate_to_base,
               start_date, source_id, insert_id, update_id)
            SELECT
              wp.products_sk        AS product_sk,
              s.fetched_at          AS fetched_at,
              s.rate                AS rate_to_base,
              %s                    AS start_date,
              2                     AS source_id,
              %s                    AS insert_id,
              NULL                  AS update_id
            FROM staging.exchange_rates_raw s
            JOIN public.products p
              ON s.base_currency    = 'USD'
             AND s.target_currency  = p.currency
            JOIN warehouse.products wp
              ON p.product_id = wp.product_id
             AND wp.end_date = '9999-12-31';
            """,
            (load_ts, run_etl_id)
        )
        conn.commit()

        # ------------------------------------------------------------------------------
        # 11) Reset exchange_rates surrogate sequence
        # ------------------------------------------------------------------------------
        cur.execute(
            """
            SELECT setval(
                     pg_get_serial_sequence('warehouse.exchange_rates','exchange_rates_sk'),
                     COALESCE((SELECT MAX(exchange_rates_sk) FROM warehouse.exchange_rates), 1),
                     true
                   );
            """
        )
        conn.commit()

        # ------------------------------------------------------------------------------
        # 12) Completion message
        # ------------------------------------------------------------------------------
        print("Full warehouse load complete. (All new rows inserted with insert_id =", run_etl_id, ")")
        cur.close()

    except Exception as e:
        print("ERROR during full warehouse load:", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    full_load_warehouse()
