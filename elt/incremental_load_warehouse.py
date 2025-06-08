# etl_scripts/incremental_load_warehouse.py

import psycopg2
from datetime import datetime
from config.settings import DB_CONFIG

def get_next_etl_id(cur):
    """
    Ensure a sequence 'warehouse.etl_seq' exists and return the next value.
    Now accounts for both insert_id AND update_id, so that the sequence
    always increments beyond any previously used ID.
    """
    cur.execute("""
        CREATE SEQUENCE IF NOT EXISTS warehouse.etl_seq START WITH 1;
    """)
    # Find the greatest value among all insert_id and update_id columns:
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

    # Advance the sequence to next_id (so nextval will return next_id)
    cur.execute("SELECT setval('warehouse.etl_seq', %s, false);", (next_id,))
    cur.execute("SELECT nextval('warehouse.etl_seq');")
    return cur.fetchone()[0]


def process_table_categories(cur, load_ts):
    """
    Compare public.categories to warehouse.categories.
    Use category_id (natural) to find matches; insert or expire versions using surrogate key.
    """
    # 1) Fetch “current” warehouse rows:
    cur.execute("""
        SELECT categories_sk, category_id, category_name
        FROM warehouse.categories
        WHERE end_date = '9999-12-31';
    """)
    wh_current = {row[1]: (row[0], row[2]) for row in cur.fetchall()}
    # wh_current: { category_id: (categories_sk, category_name) }

    # 2) Fetch all rows from public.categories:
    cur.execute("SELECT category_id, category_name FROM public.categories;")
    src = {row[0]: row[1] for row in cur.fetchall()}

    # 3) INSERT new category_ids:
    for cid, cname in src.items():
        if cid not in wh_current:
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                INSERT INTO warehouse.categories
                  (category_id, category_name, start_date, source_id, insert_id, update_id)
                VALUES (%s, %s, %s, 1, %s, NULL);
            """, (cid, cname, load_ts, etl_id))

    # 4) UPDATE changed names (expire old version, insert new):
    for cid, cname in src.items():
        if cid in wh_current and wh_current[cid][1] != cname:
            sk, _old_name = wh_current[cid]
            etl_id = get_next_etl_id(cur)
            # expire old version
            cur.execute("""
                UPDATE warehouse.categories
                   SET end_date = %s,
                       update_id = %s
                 WHERE categories_sk = %s
                   AND end_date = '9999-12-31';
            """, (load_ts, etl_id, sk))
            # insert new version
            cur.execute("""
                INSERT INTO warehouse.categories
                  (category_id, category_name, start_date, source_id, insert_id, update_id)
                VALUES (%s, %s, %s, 1, %s, NULL);
            """, (cid, cname, load_ts, etl_id))

    # 5) DELETEd in source (expire current version):
    for cid, (sk, _old_name) in wh_current.items():
        if cid not in src:
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                UPDATE warehouse.categories
                   SET end_date = %s,
                       update_id = %s
                 WHERE categories_sk = %s
                   AND end_date = '9999-12-31';
            """, (load_ts, etl_id, sk))


def process_table_users(cur, load_ts):
    """
    Compare public.users → warehouse.users via user_id.
    Insert new, expire & re-insert changed, expire deleted.
    """
    cur.execute("""
        SELECT users_sk, user_id, user_name
        FROM warehouse.users
        WHERE end_date = '9999-12-31';
    """)
    wh_current = {row[1]: (row[0], row[2]) for row in cur.fetchall()}
    # wh_current: { user_id: (users_sk, user_name) }

    cur.execute("SELECT user_id, user_name FROM public.users;")
    src = {row[0]: row[1] for row in cur.fetchall()}

    # 1) INSERT new
    for uid, uname in src.items():
        if uid not in wh_current:
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                INSERT INTO warehouse.users
                  (user_id, user_name, start_date, source_id, insert_id, update_id)
                VALUES (%s, %s, %s, 1, %s, NULL);
            """, (uid, uname, load_ts, etl_id))

    # 2) UPDATE changed (expire & re‐insert)
    for uid, uname in src.items():
        if uid in wh_current and wh_current[uid][1] != uname:
            sk, _old_name = wh_current[uid]
            etl_id = get_next_etl_id(cur)
            # expire old row
            cur.execute("""
                UPDATE warehouse.users
                   SET end_date = %s,
                       update_id = %s
                 WHERE users_sk = %s
                   AND end_date = '9999-12-31';
            """, (load_ts, etl_id, sk))
            # insert new version
            cur.execute("""
                INSERT INTO warehouse.users
                  (user_id, user_name, start_date, source_id, insert_id, update_id)
                VALUES (%s, %s, %s, 1, %s, NULL);
            """, (uid, uname, load_ts, etl_id))

    # 3) DELETEd (expire current)
    for uid, (sk, _old_name) in wh_current.items():
        if uid not in src:
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                UPDATE warehouse.users
                   SET end_date = %s,
                       update_id = %s
                 WHERE users_sk = %s
                   AND end_date = '9999-12-31';
            """, (load_ts, etl_id, sk))


def process_table_products(cur, load_ts):
    """
    Compare public.products → warehouse.products by product_id.
    Must lookup category_sk for each row; handle insert/update/delete.
    """
    cur.execute("""
        SELECT p.products_sk, p.product_id, p.product_name, p.category_sk,
               p.discounted_price, p.actual_price, p.discount_percentage,
               p.rating, p.rating_count, p.about_product, p.product_link, p.currency
        FROM warehouse.products p
        WHERE p.end_date = '9999-12-31';
    """)
    wh_current = {
        row[1]: {
            'sk': row[0],
            'product_name': row[2],
            'category_sk': row[3],
            'discounted_price': row[4],
            'actual_price': row[5],
            'discount_percentage': row[6],
            'rating': row[7],
            'rating_count': row[8],
            'about_product': row[9],
            'product_link': row[10],
            'currency': row[11]
        }
        for row in cur.fetchall()
    }
    # wh_current keyed by product_id

    cur.execute("""
        SELECT product_id, product_name, category_id,
               discounted_price, actual_price, discount_percentage,
               rating, rating_count, about_product, product_link, currency
        FROM public.products;
    """)
    src_raw = {row[0]: row[1:] for row in cur.fetchall()}
    # src_raw maps product_id → tuple(...)

    for pid, vals in src_raw.items():
        (pname, cat_id, dp, ap, d_pct, r, r_cnt, about, link, curr) = vals
        # lookup category_sk of current version
        cur.execute("""
            SELECT categories_sk
            FROM warehouse.categories
            WHERE category_id = %s
              AND end_date = '9999-12-31';
        """, (cat_id,))
        cat_row = cur.fetchone()
        if not cat_row:
            continue
        category_sk = cat_row[0]

        if pid not in wh_current:
            # INSERT new
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                INSERT INTO warehouse.products
                  (product_id, product_name, category_sk,
                   discounted_price, actual_price, discount_percentage,
                   rating, rating_count, about_product, product_link, currency,
                   start_date, source_id, insert_id, update_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, NULL);
            """, (pid, pname, category_sk,
                  dp, ap, d_pct,
                  r, r_cnt, about, link, curr,
                  load_ts, etl_id))
        else:
            old = wh_current[pid]
            # compare every attribute except surrogate sk
            if (
                old['product_name'] != pname or
                old['category_sk'] != category_sk or
                old['discounted_price'] != dp or
                old['actual_price'] != ap or
                old['discount_percentage'] != d_pct or
                old['rating'] != r or
                old['rating_count'] != r_cnt or
                old['about_product'] != about or
                old['product_link'] != link or
                old['currency'] != curr
            ):
                sk = old['sk']
                etl_id = get_next_etl_id(cur)
                # expire old
                cur.execute("""
                    UPDATE warehouse.products
                       SET end_date = %s,
                           update_id = %s
                     WHERE products_sk = %s
                       AND end_date = '9999-12-31';
                """, (load_ts, etl_id, sk))
                # insert new version
                cur.execute("""
                    INSERT INTO warehouse.products
                      (product_id, product_name, category_sk,
                       discounted_price, actual_price, discount_percentage,
                       rating, rating_count, about_product, product_link, currency,
                       start_date, source_id, insert_id, update_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, NULL);
                """, (pid, pname, category_sk,
                      dp, ap, d_pct,
                      r, r_cnt, about, link, curr,
                      load_ts, etl_id))

    # DELETEd
    for pid, old in wh_current.items():
        if pid not in src_raw:
            sk = old['sk']
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                UPDATE warehouse.products
                   SET end_date = %s,
                       update_id = %s
                 WHERE products_sk = %s
                   AND end_date = '9999-12-31';
            """, (load_ts, etl_id, sk))


def process_table_reviews(cur, load_ts):
    """
    Compare public.reviews → warehouse.reviews by review_id.
    Must lookup product_sk and user_sk; handle insert/update/delete.
    """
    cur.execute("""
        SELECT r.reviews_sk, r.review_id, r.product_sk, r.user_sk,
               r.review_title, r.review_content
        FROM warehouse.reviews r
        WHERE r.end_date = '9999-12-31';
    """)
    wh_current = {
        row[1]: {
            'sk': row[0],
            'product_sk': row[2],
            'user_sk': row[3],
            'review_title': row[4],
            'review_content': row[5]
        }
        for row in cur.fetchall()
    }

    cur.execute("""
        SELECT review_id, product_id, user_id, review_title, review_content
        FROM public.reviews;
    """)
    src_raw = {row[0]: (row[1], row[2], row[3], row[4]) for row in cur.fetchall()}

    for rid, (prod_id, usr_id, title, content) in src_raw.items():
        # lookup product_sk
        cur.execute("""
            SELECT products_sk
            FROM warehouse.products
            WHERE product_id = %s AND end_date = '9999-12-31';
        """, (prod_id,))
        prod_row = cur.fetchone()
        if not prod_row:
            continue
        product_sk = prod_row[0]

        # lookup user_sk
        cur.execute("""
            SELECT users_sk
            FROM warehouse.users
            WHERE user_id = %s AND end_date = '9999-12-31';
        """, (usr_id,))
        user_row = cur.fetchone()
        if not user_row:
            continue
        user_sk = user_row[0]

        if rid not in wh_current:
            # INSERT new
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                INSERT INTO warehouse.reviews
                  (review_id, product_sk, user_sk, review_title, review_content,
                   start_date, source_id, insert_id, update_id)
                VALUES (%s, %s, %s, %s, %s, %s, 1, %s, NULL);
            """, (rid, product_sk, user_sk, title, content, load_ts, etl_id))
        else:
            old = wh_current[rid]
            if (
                old['product_sk'] != product_sk or
                old['user_sk'] != user_sk or
                old['review_title'] != title or
                old['review_content'] != content
            ):
                sk = old['sk']
                etl_id = get_next_etl_id(cur)
                # expire old
                cur.execute("""
                    UPDATE warehouse.reviews
                       SET end_date = %s,
                           update_id = %s
                     WHERE reviews_sk = %s
                       AND end_date = '9999-12-31';
                """, (load_ts, etl_id, sk))
                # insert new
                cur.execute("""
                    INSERT INTO warehouse.reviews
                      (review_id, product_sk, user_sk, review_title, review_content,
                       start_date, source_id, insert_id, update_id)
                    VALUES (%s, %s, %s, %s, %s, %s, 1, %s, NULL);
                """, (rid, product_sk, user_sk, title, content, load_ts, etl_id))

    # DELETEd
    for rid, old in wh_current.items():
        if rid not in src_raw:
            sk = old['sk']
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                UPDATE warehouse.reviews
                   SET end_date = %s,
                       update_id = %s
                 WHERE reviews_sk = %s
                   AND end_date = '9999-12-31';
            """, (load_ts, etl_id, sk))


def process_table_locations(cur, load_ts):
    """
    Compare public.locations → warehouse.locations by location_id.
    Lookup product_sk; handle insert/update/delete.
    """
    cur.execute("""
        SELECT l.locations_sk, l.location_id, l.product_sk, l.country, l.city
        FROM warehouse.locations l
        WHERE l.end_date = '9999-12-31';
    """)
    wh_current = {
        row[1]: {
            'sk': row[0],
            'product_sk': row[2],
            'country': row[3],
            'city': row[4]
        }
        for row in cur.fetchall()
    }

    cur.execute("""
        SELECT location_id, product_id, country, city
        FROM public.locations;
    """)
    src_raw = {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}

    for lid, (prod_id, country, city) in src_raw.items():
        # lookup product_sk
        cur.execute("""
            SELECT products_sk
            FROM warehouse.products
            WHERE product_id = %s AND end_date = '9999-12-31';
        """, (prod_id,))
        prod_row = cur.fetchone()
        if not prod_row:
            continue
        product_sk = prod_row[0]

        if lid not in wh_current:
            # INSERT new
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                INSERT INTO warehouse.locations
                  (location_id, product_sk, country, city,
                   start_date, source_id, insert_id, update_id)
                VALUES (%s, %s, %s, %s, %s, 1, %s, NULL);
            """, (lid, product_sk, country, city, load_ts, etl_id))
        else:
            old = wh_current[lid]
            if (
                old['product_sk'] != product_sk or
                old['country'] != country or
                old['city'] != city
            ):
                sk = old['sk']
                etl_id = get_next_etl_id(cur)
                cur.execute("""
                    UPDATE warehouse.locations
                       SET end_date = %s,
                           update_id = %s
                     WHERE locations_sk = %s
                       AND end_date = '9999-12-31';
                """, (load_ts, etl_id, sk))
                cur.execute("""
                    INSERT INTO warehouse.locations
                      (location_id, product_sk, country, city,
                       start_date, source_id, insert_id, update_id)
                    VALUES (%s, %s, %s, %s, %s, 1, %s, NULL);
                """, (lid, product_sk, country, city, load_ts, etl_id))

    # DELETEd
    for lid, old in wh_current.items():
        if lid not in src_raw:
            sk = old['sk']
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                UPDATE warehouse.locations
                   SET end_date = %s,
                       update_id = %s
                 WHERE locations_sk = %s
                   AND end_date = '9999-12-31';
            """, (load_ts, etl_id, sk))


def process_table_exchange_rates(cur, load_ts):
    """
    Take the latest staging.exchange_rates_raw rates and upsert into warehouse.exchange_rates.
    Must look up product_sk by product_id (natural).
    """
    # Build a map: product_id → (fetched_at, rate) of the latest staging rows
    cur.execute("""
        SELECT p.product_id, s.fetched_at, s.rate
        FROM (
          SELECT target_currency, MAX(fetched_at) AS max_fetched
          FROM staging.exchange_rates_raw
          WHERE base_currency = 'USD'
          GROUP BY target_currency
        ) AS latest
        JOIN staging.exchange_rates_raw s
          ON s.target_currency = latest.target_currency
         AND s.fetched_at = latest.max_fetched
        JOIN public.products p
          ON s.target_currency = p.currency;
    """)
    src = {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    # src: {product_id: (fetched_at, rate)}

    # 1) Fetch “current” warehouse rates
    cur.execute("""
        SELECT er.exchange_rates_sk, er.product_sk, er.fetched_at, er.rate_to_base
        FROM warehouse.exchange_rates er
        WHERE er.end_date = '9999-12-31';
    """)
    wh_current = {row[1]: (row[0], row[2], row[3]) for row in cur.fetchall()}
    # wh_current: {product_sk: (exchange_rates_sk, fetched_at, rate_to_base)}

    # 2) For each src entry, lookup product_sk and handle insert/update
    for prod_id, (fetched_at, rate) in src.items():
        cur.execute("""
            SELECT products_sk
            FROM warehouse.products
            WHERE product_id = %s AND end_date = '9999-12-31';
        """, (prod_id,))
        prod_row = cur.fetchone()
        if not prod_row:
            continue
        product_sk = prod_row[0]

        if product_sk not in wh_current:
            # INSERT new
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                INSERT INTO warehouse.exchange_rates
                  (product_sk, fetched_at, rate_to_base,
                   start_date, source_id, insert_id, update_id)
                VALUES (%s, %s, %s, %s, 2, %s, NULL);
            """, (product_sk, fetched_at, rate, load_ts, etl_id))
        else:
            old_sk, old_fetched, old_rate = wh_current[product_sk]
            if old_fetched != fetched_at or old_rate != rate:
                etl_id = get_next_etl_id(cur)
                # expire old version
                cur.execute("""
                    UPDATE warehouse.exchange_rates
                       SET end_date = %s,
                           update_id = %s
                     WHERE exchange_rates_sk = %s
                       AND end_date = '9999-12-31';
                """, (load_ts, etl_id, old_sk))
                # insert new version
                cur.execute("""
                    INSERT INTO warehouse.exchange_rates
                      (product_sk, fetched_at, rate_to_base,
                       start_date, source_id, insert_id, update_id)
                    VALUES (%s, %s, %s, %s, 2, %s, NULL);
                """, (product_sk, fetched_at, rate, load_ts, etl_id))

    # 3) DELETEd: any product_sk in wh_current not in src
    for product_sk, (old_sk, _, _) in wh_current.items():
        # map product_sk back to product_id for comparison
        cur.execute("""
            SELECT product_id
            FROM warehouse.products
            WHERE products_sk = %s;
        """, (product_sk,))
        prod_id = cur.fetchone()[0]
        if prod_id not in src:
            etl_id = get_next_etl_id(cur)
            cur.execute("""
                UPDATE warehouse.exchange_rates
                   SET end_date = %s,
                       update_id = %s
                 WHERE exchange_rates_sk = %s
                   AND end_date = '9999-12-31';
            """, (load_ts, etl_id, old_sk))


def incremental_load_warehouse():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1) Do NOT truncate any warehouse tables here. We want to preserve existing rows
        #    so that insert_id remains intact for previously loaded records.

        # 2) Capture load timestamp
        load_ts = datetime.utcnow()

        # 3) Process each table
        process_table_categories(cur, load_ts)
        conn.commit()

        process_table_users(cur, load_ts)
        conn.commit()

        process_table_products(cur, load_ts)
        conn.commit()

        process_table_reviews(cur, load_ts)
        conn.commit()

        process_table_locations(cur, load_ts)
        conn.commit()

        process_table_exchange_rates(cur, load_ts)
        conn.commit()

        print("Incremental load completed successfully.")
        cur.close()
    except Exception as e:
        print("ERROR during incremental warehouse load:", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    incremental_load_warehouse()
