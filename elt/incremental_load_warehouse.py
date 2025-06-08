# etl_scripts/incremental_load_warehouse.py

import psycopg2
from datetime import datetime
from config.settings import DB_CONFIG

def get_next_etl_id(cur):
    """
    Ensure a sequence 'warehouse.etl_seq' exists and return the next value.
    Accounts for both insert_id AND update_id, so that the sequence
    always increments beyond any previously used ID.
    """
    cur.execute("CREATE SEQUENCE IF NOT EXISTS warehouse.etl_seq START WITH 1;")
    cur.execute("""
        SELECT GREATEST(
          COALESCE((SELECT MAX(insert_id)   FROM warehouse.categories), 0),
          COALESCE((SELECT MAX(update_id)   FROM warehouse.categories), 0),
          COALESCE((SELECT MAX(insert_id)   FROM warehouse.users),      0),
          COALESCE((SELECT MAX(update_id)   FROM warehouse.users),      0),
          COALESCE((SELECT MAX(insert_id)   FROM warehouse.products),   0),
          COALESCE((SELECT MAX(update_id)   FROM warehouse.products),   0),
          COALESCE((SELECT MAX(insert_id)   FROM warehouse.reviews),    0),
          COALESCE((SELECT MAX(update_id)   FROM warehouse.reviews),    0),
          COALESCE((SELECT MAX(insert_id)   FROM warehouse.locations),  0),
          COALESCE((SELECT MAX(update_id)   FROM warehouse.locations),  0),
          COALESCE((SELECT MAX(insert_id)   FROM warehouse.exchange_rates), 0),
          COALESCE((SELECT MAX(update_id)   FROM warehouse.exchange_rates), 0)
        );
    """)
    max_id = cur.fetchone()[0] or 0
    next_id = max_id + 1
    # Advance the sequence so nextval returns next_id
    cur.execute("SELECT setval('warehouse.etl_seq', %s, false);", (next_id,))
    cur.execute("SELECT nextval('warehouse.etl_seq');")
    return cur.fetchone()[0]


def process_table_categories(cur, load_ts, run_etl_id):
    # Fetch current
    cur.execute("""
        SELECT categories_sk, category_id, category_name
          FROM warehouse.categories
         WHERE end_date = '9999-12-31';
    """)
    wh = {row[1]:(row[0],row[2]) for row in cur.fetchall()}

    # Fetch source
    cur.execute("SELECT category_id, category_name FROM public.categories;")
    src = {row[0]:row[1] for row in cur.fetchall()}

    # INSERT new
    for cid, cname in src.items():
        if cid not in wh:
            cur.execute("""
                INSERT INTO warehouse.categories
                  (category_id, category_name, start_date, source_id, insert_id, update_id)
                VALUES (%s,%s,%s,1,%s,NULL);
            """, (cid,cname,load_ts,run_etl_id))

    # UPDATE changed
    for cid, cname in src.items():
        if cid in wh and wh[cid][1] != cname:
            sk,_ = wh[cid]
            cur.execute("""
                UPDATE warehouse.categories
                   SET end_date=%s, update_id=%s
                 WHERE categories_sk=%s AND end_date='9999-12-31';
            """, (load_ts,run_etl_id,sk))
            cur.execute("""
                INSERT INTO warehouse.categories
                  (category_id, category_name, start_date, source_id, insert_id, update_id)
                VALUES (%s,%s,%s,1,%s,NULL);
            """, (cid,cname,load_ts,run_etl_id))

    # DELETEd
    for cid,(sk,_) in wh.items():
        if cid not in src:
            cur.execute("""
                UPDATE warehouse.categories
                   SET end_date=%s, update_id=%s
                 WHERE categories_sk=%s AND end_date='9999-12-31';
            """, (load_ts,run_etl_id,sk))


def process_table_users(cur, load_ts, run_etl_id):
    cur.execute("""
        SELECT users_sk, user_id, user_name
          FROM warehouse.users
         WHERE end_date = '9999-12-31';
    """)
    wh = {row[1]:(row[0],row[2]) for row in cur.fetchall()}

    cur.execute("SELECT user_id, user_name FROM public.users;")
    src = {row[0]:row[1] for row in cur.fetchall()}

    for uid,uname in src.items():
        if uid not in wh:
            cur.execute("""
                INSERT INTO warehouse.users
                  (user_id,user_name,start_date,source_id,insert_id,update_id)
                VALUES (%s,%s,%s,1,%s,NULL);
            """,(uid,uname,load_ts,run_etl_id))

    for uid,uname in src.items():
        if uid in wh and wh[uid][1]!=uname:
            sk,_=wh[uid]
            cur.execute("""
                UPDATE warehouse.users
                   SET end_date=%s, update_id=%s
                 WHERE users_sk=%s AND end_date='9999-12-31';
            """,(load_ts,run_etl_id,sk))
            cur.execute("""
                INSERT INTO warehouse.users
                  (user_id,user_name,start_date,source_id,insert_id,update_id)
                VALUES (%s,%s,%s,1,%s,NULL);
            """,(uid,uname,load_ts,run_etl_id))

    for uid,(sk,_) in wh.items():
        if uid not in src:
            cur.execute("""
                UPDATE warehouse.users
                   SET end_date=%s, update_id=%s
                 WHERE users_sk=%s AND end_date='9999-12-31';
            """,(load_ts,run_etl_id,sk))


def process_table_products(cur, load_ts, run_etl_id):
    cur.execute("""
        SELECT p.products_sk, p.product_id, p.product_name, p.category_sk,
               p.discounted_price, p.actual_price, p.discount_percentage,
               p.rating, p.rating_count, p.about_product, p.product_link, p.currency
          FROM warehouse.products p
         WHERE p.end_date = '9999-12-31';
    """)
    wh = {r[1]:{
        'sk':r[0],'product_name':r[2],'category_sk':r[3],
        'discounted_price':r[4],'actual_price':r[5],
        'discount_percentage':r[6],'rating':r[7],
        'rating_count':r[8],'about_product':r[9],
        'product_link':r[10],'currency':r[11]
    } for r in cur.fetchall()}

    cur.execute("""
        SELECT product_id,product_name,category_id,
               discounted_price,actual_price,discount_percentage,
               rating,rating_count,about_product,product_link,currency
          FROM public.products;
    """)
    src = {r[0]:r[1:] for r in cur.fetchall()}

    for pid,vals in src.items():
        pname,cat_id,dp,ap,d_pct,r_cnt_r,rc,about,link,curr = vals
        # lookup category_sk
        cur.execute("""
            SELECT categories_sk
              FROM warehouse.categories
             WHERE category_id=%s AND end_date='9999-12-31';
        """,(cat_id,))
        row=cur.fetchone()
        if not row: continue
        category_sk=row[0]

        if pid not in wh:
            cur.execute("""
                INSERT INTO warehouse.products
                  (product_id,product_name,category_sk,
                   discounted_price,actual_price,discount_percentage,
                   rating,rating_count,about_product,product_link,currency,
                   start_date,source_id,insert_id,update_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,NULL);
            """,(pid,pname,category_sk,dp,ap,d_pct,r_cnt_r,rc,about,link,curr,load_ts,run_etl_id))
        else:
            old=wh[pid]
            if (
                old['product_name']!=pname or
                old['category_sk']!=category_sk or
                old['discounted_price']!=dp or
                old['actual_price']!=ap or
                old['discount_percentage']!=d_pct or
                old['rating']!=r_cnt_r or
                old['rating_count']!=rc or
                old['about_product']!=about or
                old['product_link']!=link or
                old['currency']!=curr
            ):
                sk=old['sk']
                cur.execute("""
                    UPDATE warehouse.products
                       SET end_date=%s, update_id=%s
                     WHERE products_sk=%s AND end_date='9999-12-31';
                """,(load_ts,run_etl_id,sk))
                cur.execute("""
                    INSERT INTO warehouse.products
                      (product_id,product_name,category_sk,
                       discounted_price,actual_price,discount_percentage,
                       rating,rating_count,about_product,product_link,currency,
                       start_date,source_id,insert_id,update_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,NULL);
                """,(pid,pname,category_sk,dp,ap,d_pct,r_cnt_r,rc,about,link,curr,load_ts,run_etl_id))

    for pid,old in wh.items():
        if pid not in src:
            cur.execute("""
                UPDATE warehouse.products
                   SET end_date=%s, update_id=%s
                 WHERE products_sk=%s AND end_date='9999-12-31';
            """,(load_ts,run_etl_id,old['sk']))


def process_table_reviews(cur, load_ts, run_etl_id):
    cur.execute("""
        SELECT reviews_sk, review_id, product_sk, user_sk, review_title, review_content
          FROM warehouse.reviews
         WHERE end_date='9999-12-31';
    """)
    wh = {r[1]:{'sk':r[0],'product_sk':r[2],'user_sk':r[3],'review_title':r[4],'review_content':r[5]}
          for r in cur.fetchall()}

    cur.execute("SELECT review_id,product_id,user_id,review_title,review_content FROM public.reviews;")
    src = {r[0]:(r[1],r[2],r[3],r[4]) for r in cur.fetchall()}

    for rid,(prod_id,usr_id,title,content) in src.items():
        # lookup product_sk
        cur.execute("SELECT products_sk FROM warehouse.products WHERE product_id=%s AND end_date='9999-12-31';",(prod_id,))
        prow=cur.fetchone();
        if not prow: continue
        product_sk=prow[0]
        # lookup user_sk
        cur.execute("SELECT users_sk FROM warehouse.users WHERE user_id=%s AND end_date='9999-12-31';",(usr_id,))
        urow=cur.fetchone();
        if not urow: continue
        user_sk=urow[0]

        if rid not in wh:
            cur.execute("""
                INSERT INTO warehouse.reviews
                  (review_id,product_sk,user_sk,review_title,review_content,
                   start_date,source_id,insert_id,update_id)
                VALUES (%s,%s,%s,%s,%s,%s,1,%s,NULL);
            """,(rid,product_sk,user_sk,title,content,load_ts,run_etl_id))
        else:
            old=wh[rid]
            if (
                old['product_sk']!=product_sk or
                old['user_sk']!=user_sk or
                old['review_title']!=title or
                old['review_content']!=content
            ):
                sk=old['sk']
                cur.execute("""
                    UPDATE warehouse.reviews
                       SET end_date=%s, update_id=%s
                     WHERE reviews_sk=%s AND end_date='9999-12-31';
                """,(load_ts,run_etl_id,sk))
                cur.execute("""
                    INSERT INTO warehouse.reviews
                      (review_id,product_sk,user_sk,review_title,review_content,
                       start_date,source_id,insert_id,update_id)
                    VALUES (%s,%s,%s,%s,%s,%s,1,%s,NULL);
                """,(rid,product_sk,user_sk,title,content,load_ts,run_etl_id))

    for rid,old in wh.items():
        if rid not in src:
            cur.execute("""
                UPDATE warehouse.reviews
                   SET end_date=%s, update_id=%s
                 WHERE reviews_sk=%s AND end_date='9999-12-31';
            """,(load_ts,run_etl_id,old['sk']))


def process_table_locations(cur, load_ts, run_etl_id):
    cur.execute("""
        SELECT locations_sk, location_id, product_sk, country, city
          FROM warehouse.locations
         WHERE end_date='9999-12-31';
    """)
    wh = {r[1]:{'sk':r[0],'product_sk':r[2],'country':r[3],'city':r[4]}
          for r in cur.fetchall()}

    cur.execute("SELECT location_id,product_id,country,city FROM public.locations;")
    src = {r[0]:(r[1],r[2],r[3]) for r in cur.fetchall()}

    for lid,(prod_id,country,city) in src.items():
        cur.execute("SELECT products_sk FROM warehouse.products WHERE product_id=%s AND end_date='9999-12-31';",(prod_id,))
        prow=cur.fetchone();
        if not prow: continue
        product_sk=prow[0]

        if lid not in wh:
            cur.execute("""
                INSERT INTO warehouse.locations
                  (location_id,product_sk,country,city,start_date,source_id,insert_id,update_id)
                VALUES (%s,%s,%s,%s,%s,1,%s,NULL);
            """,(lid,product_sk,country,city,load_ts,run_etl_id))
        else:
            old=wh[lid]
            if (
                old['product_sk']!=product_sk or
                old['country']!=country or
                old['city']!=city
            ):
                sk=old['sk']
                cur.execute("""
                    UPDATE warehouse.locations
                       SET end_date=%s, update_id=%s
                     WHERE locations_sk=%s AND end_date='9999-12-31';
                """,(load_ts,run_etl_id,sk))
                cur.execute("""
                    INSERT INTO warehouse.locations
                      (location_id,product_sk,country,city,start_date,source_id,insert_id,update_id)
                    VALUES (%s,%s,%s,%s,%s,1,%s,NULL);
                """,(lid,product_sk,country,city,load_ts,run_etl_id))

    for lid,old in wh.items():
        if lid not in src:
            cur.execute("""
                UPDATE warehouse.locations
                   SET end_date=%s, update_id=%s
                 WHERE locations_sk=%s AND end_date='9999-12-31';
            """,(load_ts,run_etl_id,old['sk']))


def process_table_exchange_rates(cur, load_ts, run_etl_id):
    cur.execute("""
        SELECT p.product_id, s.fetched_at, s.rate
          FROM (
                SELECT target_currency, MAX(fetched_at) AS max_fetched
                  FROM staging.exchange_rates_raw
                 WHERE base_currency='USD'
                 GROUP BY target_currency
               ) AS latest
          JOIN staging.exchange_rates_raw s
            ON s.target_currency=latest.target_currency
           AND s.fetched_at=latest.max_fetched
          JOIN public.products p
            ON s.target_currency=p.currency;
    """)
    src={r[0]:(r[1],r[2]) for r in cur.fetchall()}

    cur.execute("""
        SELECT exchange_rates_sk, product_sk, fetched_at, rate_to_base
          FROM warehouse.exchange_rates
         WHERE end_date='9999-12-31';
    """)
    wh={r[1]:(r[0],r[2],r[3]) for r in cur.fetchall()}

    for prod_id,(fetched_at,rate) in src.items():
        cur.execute("SELECT products_sk FROM warehouse.products WHERE product_id=%s AND end_date='9999-12-31';",(prod_id,))
        prow=cur.fetchone();
        if not prow: continue
        product_sk=prow[0]

        if product_sk not in wh:
            cur.execute("""
                INSERT INTO warehouse.exchange_rates
                  (product_sk,fetched_at,rate_to_base,start_date,source_id,insert_id,update_id)
                VALUES (%s,%s,%s,%s,2,%s,NULL);
            """,(product_sk,fetched_at,rate,load_ts,run_etl_id))
        else:
            old_sk,old_fetched,old_rate=wh[product_sk]
            if old_fetched!=fetched_at or old_rate!=rate:
                cur.execute("""
                    UPDATE warehouse.exchange_rates
                       SET end_date=%s, update_id=%s
                     WHERE exchange_rates_sk=%s AND end_date='9999-12-31';
                """,(load_ts,run_etl_id,old_sk))
                cur.execute("""
                    INSERT INTO warehouse.exchange_rates
                      (product_sk,fetched_at,rate_to_base,start_date,source_id,insert_id,update_id)
                    VALUES (%s,%s,%s,%s,2,%s,NULL);
                """,(product_sk,fetched_at,rate,load_ts,run_etl_id))

    for product_sk,(old_sk,_,_) in wh.items():
        cur.execute("SELECT product_id FROM warehouse.products WHERE products_sk=%s;",(product_sk,))
        prod_id=cur.fetchone()[0]
        if prod_id not in src:
            cur.execute("""
                UPDATE warehouse.exchange_rates
                   SET end_date=%s, update_id=%s
                 WHERE exchange_rates_sk=%s AND end_date='9999-12-31';
            """,(load_ts,run_etl_id,old_sk))


def incremental_load_warehouse():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()

        # 1) Capture timestamp
        load_ts = datetime.utcnow()

        # 2) Reserve one ETL ID for all inserts/updates this run
        run_etl_id = get_next_etl_id(cur)

        # 3) Apply each table’s logic with the same run_etl_id
        process_table_categories(cur, load_ts, run_etl_id); conn.commit()
        process_table_users(cur, load_ts, run_etl_id);      conn.commit()
        process_table_products(cur, load_ts, run_etl_id);   conn.commit()
        process_table_reviews(cur, load_ts, run_etl_id);    conn.commit()
        process_table_locations(cur, load_ts, run_etl_id);  conn.commit()
        process_table_exchange_rates(cur, load_ts, run_etl_id); conn.commit()

        print("Incremental load completed successfully.")
    except Exception as e:
        conn.rollback()
        print("ERROR during incremental warehouse load:", e)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    incremental_load_warehouse()
