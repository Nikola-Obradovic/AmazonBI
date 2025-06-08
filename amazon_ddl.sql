--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5
-- Dumped by pg_dump version 17.5

-- Started on 2025-06-08 20:21:01

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 5064 (class 1262 OID 24850)
-- Name: amazon_products; Type: DATABASE; Schema: -; Owner: postgres
--

CREATE DATABASE amazon_products WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'English_United States.1252';


ALTER DATABASE amazon_products OWNER TO postgres;

\connect amazon_products

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 6 (class 2615 OID 26176)
-- Name: staging; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA staging;


ALTER SCHEMA staging OWNER TO postgres;

--
-- TOC entry 8 (class 2615 OID 27978)
-- Name: star; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA star;


ALTER SCHEMA star OWNER TO postgres;

--
-- TOC entry 7 (class 2615 OID 27020)
-- Name: warehouse; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA warehouse;


ALTER SCHEMA warehouse OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 221 (class 1259 OID 25863)
-- Name: categories; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.categories (
    category_id integer NOT NULL,
    category_name character varying(255) NOT NULL
);


ALTER TABLE public.categories OWNER TO postgres;

--
-- TOC entry 220 (class 1259 OID 25862)
-- Name: categories_category_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.categories_category_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.categories_category_id_seq OWNER TO postgres;

--
-- TOC entry 5065 (class 0 OID 0)
-- Dependencies: 220
-- Name: categories_category_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.categories_category_id_seq OWNED BY public.categories.category_id;


--
-- TOC entry 226 (class 1259 OID 25908)
-- Name: locations; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.locations (
    location_id integer NOT NULL,
    product_id character varying(255),
    country character varying(100),
    city character varying(100)
);


ALTER TABLE public.locations OWNER TO postgres;

--
-- TOC entry 225 (class 1259 OID 25907)
-- Name: locations_location_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.locations_location_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.locations_location_id_seq OWNER TO postgres;

--
-- TOC entry 5066 (class 0 OID 0)
-- Dependencies: 225
-- Name: locations_location_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.locations_location_id_seq OWNED BY public.locations.location_id;


--
-- TOC entry 222 (class 1259 OID 25871)
-- Name: products; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.products (
    product_id character varying(255) NOT NULL,
    product_name character varying(255) NOT NULL,
    category_id integer,
    discounted_price numeric(10,2),
    actual_price numeric(10,2) NOT NULL,
    discount_percentage numeric(5,2),
    rating numeric(3,2),
    rating_count integer,
    about_product text,
    product_link text,
    currency character varying(10)
);


ALTER TABLE public.products OWNER TO postgres;

--
-- TOC entry 224 (class 1259 OID 25890)
-- Name: reviews; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.reviews (
    review_id character varying(255) NOT NULL,
    product_id character varying(255),
    user_id character varying(255),
    review_title character varying(255),
    review_content text
);


ALTER TABLE public.reviews OWNER TO postgres;

--
-- TOC entry 223 (class 1259 OID 25883)
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    user_id character varying(255) NOT NULL,
    user_name character varying(255) NOT NULL
);


ALTER TABLE public.users OWNER TO postgres;

--
-- TOC entry 227 (class 1259 OID 26177)
-- Name: exchange_rates_raw; Type: TABLE; Schema: staging; Owner: postgres
--

CREATE TABLE staging.exchange_rates_raw (
    fetched_at timestamp without time zone NOT NULL,
    base_currency character(3) NOT NULL,
    target_currency character(3) NOT NULL,
    rate numeric(18,8) NOT NULL
);


ALTER TABLE staging.exchange_rates_raw OWNER TO postgres;

--
-- TOC entry 244 (class 1259 OID 27989)
-- Name: dim_category; Type: TABLE; Schema: star; Owner: postgres
--

CREATE TABLE star.dim_category (
    category_sk integer NOT NULL,
    category_id integer NOT NULL,
    category_name character varying(255) NOT NULL
);


ALTER TABLE star.dim_category OWNER TO postgres;

--
-- TOC entry 243 (class 1259 OID 27988)
-- Name: dim_category_category_sk_seq; Type: SEQUENCE; Schema: star; Owner: postgres
--

CREATE SEQUENCE star.dim_category_category_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE star.dim_category_category_sk_seq OWNER TO postgres;

--
-- TOC entry 5067 (class 0 OID 0)
-- Dependencies: 243
-- Name: dim_category_category_sk_seq; Type: SEQUENCE OWNED BY; Schema: star; Owner: postgres
--

ALTER SEQUENCE star.dim_category_category_sk_seq OWNED BY star.dim_category.category_sk;


--
-- TOC entry 242 (class 1259 OID 27980)
-- Name: dim_date; Type: TABLE; Schema: star; Owner: postgres
--

CREATE TABLE star.dim_date (
    date_sk integer NOT NULL,
    full_date date NOT NULL,
    year integer NOT NULL,
    quarter integer NOT NULL,
    month integer NOT NULL,
    day integer NOT NULL,
    day_of_week integer NOT NULL
);


ALTER TABLE star.dim_date OWNER TO postgres;

--
-- TOC entry 241 (class 1259 OID 27979)
-- Name: dim_date_date_sk_seq; Type: SEQUENCE; Schema: star; Owner: postgres
--

CREATE SEQUENCE star.dim_date_date_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE star.dim_date_date_sk_seq OWNER TO postgres;

--
-- TOC entry 5068 (class 0 OID 0)
-- Dependencies: 241
-- Name: dim_date_date_sk_seq; Type: SEQUENCE OWNED BY; Schema: star; Owner: postgres
--

ALTER SEQUENCE star.dim_date_date_sk_seq OWNED BY star.dim_date.date_sk;


--
-- TOC entry 250 (class 1259 OID 28025)
-- Name: dim_location; Type: TABLE; Schema: star; Owner: postgres
--

CREATE TABLE star.dim_location (
    location_sk integer NOT NULL,
    location_id integer NOT NULL,
    country character varying(100),
    city character varying(100)
);


ALTER TABLE star.dim_location OWNER TO postgres;

--
-- TOC entry 249 (class 1259 OID 28024)
-- Name: dim_location_location_sk_seq; Type: SEQUENCE; Schema: star; Owner: postgres
--

CREATE SEQUENCE star.dim_location_location_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE star.dim_location_location_sk_seq OWNER TO postgres;

--
-- TOC entry 5069 (class 0 OID 0)
-- Dependencies: 249
-- Name: dim_location_location_sk_seq; Type: SEQUENCE OWNED BY; Schema: star; Owner: postgres
--

ALTER SEQUENCE star.dim_location_location_sk_seq OWNED BY star.dim_location.location_sk;


--
-- TOC entry 248 (class 1259 OID 28009)
-- Name: dim_product; Type: TABLE; Schema: star; Owner: postgres
--

CREATE TABLE star.dim_product (
    product_sk integer NOT NULL,
    product_id character varying(255) NOT NULL,
    product_name character varying(255) NOT NULL,
    category_sk integer NOT NULL
);


ALTER TABLE star.dim_product OWNER TO postgres;

--
-- TOC entry 247 (class 1259 OID 28008)
-- Name: dim_product_product_sk_seq; Type: SEQUENCE; Schema: star; Owner: postgres
--

CREATE SEQUENCE star.dim_product_product_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE star.dim_product_product_sk_seq OWNER TO postgres;

--
-- TOC entry 5070 (class 0 OID 0)
-- Dependencies: 247
-- Name: dim_product_product_sk_seq; Type: SEQUENCE OWNED BY; Schema: star; Owner: postgres
--

ALTER SEQUENCE star.dim_product_product_sk_seq OWNED BY star.dim_product.product_sk;


--
-- TOC entry 246 (class 1259 OID 27998)
-- Name: dim_user; Type: TABLE; Schema: star; Owner: postgres
--

CREATE TABLE star.dim_user (
    user_sk integer NOT NULL,
    user_id character varying(255) NOT NULL,
    user_name character varying(255) NOT NULL
);


ALTER TABLE star.dim_user OWNER TO postgres;

--
-- TOC entry 245 (class 1259 OID 27997)
-- Name: dim_user_user_sk_seq; Type: SEQUENCE; Schema: star; Owner: postgres
--

CREATE SEQUENCE star.dim_user_user_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE star.dim_user_user_sk_seq OWNER TO postgres;

--
-- TOC entry 5071 (class 0 OID 0)
-- Dependencies: 245
-- Name: dim_user_user_sk_seq; Type: SEQUENCE OWNED BY; Schema: star; Owner: postgres
--

ALTER SEQUENCE star.dim_user_user_sk_seq OWNED BY star.dim_user.user_sk;


--
-- TOC entry 252 (class 1259 OID 28034)
-- Name: fact_pricing; Type: TABLE; Schema: star; Owner: postgres
--

CREATE TABLE star.fact_pricing (
    pricing_sk integer NOT NULL,
    date_sk integer NOT NULL,
    product_sk integer NOT NULL,
    actual_price numeric(10,2),
    discounted_price numeric(10,2),
    discount_percentage numeric(5,2),
    currency character varying(10),
    rate_to_base numeric(18,8)
);


ALTER TABLE star.fact_pricing OWNER TO postgres;

--
-- TOC entry 251 (class 1259 OID 28033)
-- Name: fact_pricing_pricing_sk_seq; Type: SEQUENCE; Schema: star; Owner: postgres
--

CREATE SEQUENCE star.fact_pricing_pricing_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE star.fact_pricing_pricing_sk_seq OWNER TO postgres;

--
-- TOC entry 5072 (class 0 OID 0)
-- Dependencies: 251
-- Name: fact_pricing_pricing_sk_seq; Type: SEQUENCE OWNED BY; Schema: star; Owner: postgres
--

ALTER SEQUENCE star.fact_pricing_pricing_sk_seq OWNED BY star.fact_pricing.pricing_sk;


--
-- TOC entry 229 (class 1259 OID 27022)
-- Name: categories; Type: TABLE; Schema: warehouse; Owner: postgres
--

CREATE TABLE warehouse.categories (
    categories_sk integer NOT NULL,
    category_id integer NOT NULL,
    category_name character varying(255) NOT NULL,
    start_date timestamp without time zone NOT NULL,
    end_date timestamp without time zone DEFAULT '9999-12-31 00:00:00'::timestamp without time zone NOT NULL,
    source_id integer NOT NULL,
    insert_id integer,
    update_id integer
);


ALTER TABLE warehouse.categories OWNER TO postgres;

--
-- TOC entry 228 (class 1259 OID 27021)
-- Name: categories_categories_sk_seq; Type: SEQUENCE; Schema: warehouse; Owner: postgres
--

CREATE SEQUENCE warehouse.categories_categories_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE warehouse.categories_categories_sk_seq OWNER TO postgres;

--
-- TOC entry 5073 (class 0 OID 0)
-- Dependencies: 228
-- Name: categories_categories_sk_seq; Type: SEQUENCE OWNED BY; Schema: warehouse; Owner: postgres
--

ALTER SEQUENCE warehouse.categories_categories_sk_seq OWNED BY warehouse.categories.categories_sk;


--
-- TOC entry 240 (class 1259 OID 27164)
-- Name: etl_seq; Type: SEQUENCE; Schema: warehouse; Owner: postgres
--

CREATE SEQUENCE warehouse.etl_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE warehouse.etl_seq OWNER TO postgres;

--
-- TOC entry 239 (class 1259 OID 27093)
-- Name: exchange_rates; Type: TABLE; Schema: warehouse; Owner: postgres
--

CREATE TABLE warehouse.exchange_rates (
    exchange_rates_sk integer NOT NULL,
    product_sk integer NOT NULL,
    fetched_at timestamp without time zone NOT NULL,
    rate_to_base numeric(18,8) NOT NULL,
    start_date timestamp without time zone NOT NULL,
    end_date timestamp without time zone DEFAULT '9999-12-31 00:00:00'::timestamp without time zone NOT NULL,
    source_id integer NOT NULL,
    insert_id integer,
    update_id integer
);


ALTER TABLE warehouse.exchange_rates OWNER TO postgres;

--
-- TOC entry 238 (class 1259 OID 27092)
-- Name: exchange_rates_exchange_rates_sk_seq; Type: SEQUENCE; Schema: warehouse; Owner: postgres
--

CREATE SEQUENCE warehouse.exchange_rates_exchange_rates_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE warehouse.exchange_rates_exchange_rates_sk_seq OWNER TO postgres;

--
-- TOC entry 5074 (class 0 OID 0)
-- Dependencies: 238
-- Name: exchange_rates_exchange_rates_sk_seq; Type: SEQUENCE OWNED BY; Schema: warehouse; Owner: postgres
--

ALTER SEQUENCE warehouse.exchange_rates_exchange_rates_sk_seq OWNED BY warehouse.exchange_rates.exchange_rates_sk;


--
-- TOC entry 237 (class 1259 OID 27079)
-- Name: locations; Type: TABLE; Schema: warehouse; Owner: postgres
--

CREATE TABLE warehouse.locations (
    locations_sk integer NOT NULL,
    location_id integer NOT NULL,
    product_sk integer NOT NULL,
    country character varying(100),
    city character varying(100),
    start_date timestamp without time zone NOT NULL,
    end_date timestamp without time zone DEFAULT '9999-12-31 00:00:00'::timestamp without time zone NOT NULL,
    source_id integer NOT NULL,
    insert_id integer,
    update_id integer
);


ALTER TABLE warehouse.locations OWNER TO postgres;

--
-- TOC entry 236 (class 1259 OID 27078)
-- Name: locations_locations_sk_seq; Type: SEQUENCE; Schema: warehouse; Owner: postgres
--

CREATE SEQUENCE warehouse.locations_locations_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE warehouse.locations_locations_sk_seq OWNER TO postgres;

--
-- TOC entry 5075 (class 0 OID 0)
-- Dependencies: 236
-- Name: locations_locations_sk_seq; Type: SEQUENCE OWNED BY; Schema: warehouse; Owner: postgres
--

ALTER SEQUENCE warehouse.locations_locations_sk_seq OWNED BY warehouse.locations.locations_sk;


--
-- TOC entry 233 (class 1259 OID 27042)
-- Name: products; Type: TABLE; Schema: warehouse; Owner: postgres
--

CREATE TABLE warehouse.products (
    products_sk integer NOT NULL,
    product_id character varying(255) NOT NULL,
    product_name character varying(255) NOT NULL,
    category_sk integer NOT NULL,
    discounted_price numeric(10,2),
    actual_price numeric(10,2) NOT NULL,
    discount_percentage numeric(5,2),
    rating numeric(3,2),
    rating_count integer,
    about_product text,
    product_link text,
    currency character varying(10),
    start_date timestamp without time zone NOT NULL,
    end_date timestamp without time zone DEFAULT '9999-12-31 00:00:00'::timestamp without time zone NOT NULL,
    source_id integer NOT NULL,
    insert_id integer,
    update_id integer
);


ALTER TABLE warehouse.products OWNER TO postgres;

--
-- TOC entry 232 (class 1259 OID 27041)
-- Name: products_products_sk_seq; Type: SEQUENCE; Schema: warehouse; Owner: postgres
--

CREATE SEQUENCE warehouse.products_products_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE warehouse.products_products_sk_seq OWNER TO postgres;

--
-- TOC entry 5076 (class 0 OID 0)
-- Dependencies: 232
-- Name: products_products_sk_seq; Type: SEQUENCE OWNED BY; Schema: warehouse; Owner: postgres
--

ALTER SEQUENCE warehouse.products_products_sk_seq OWNED BY warehouse.products.products_sk;


--
-- TOC entry 235 (class 1259 OID 27058)
-- Name: reviews; Type: TABLE; Schema: warehouse; Owner: postgres
--

CREATE TABLE warehouse.reviews (
    reviews_sk integer NOT NULL,
    review_id character varying(255) NOT NULL,
    product_sk integer NOT NULL,
    user_sk integer NOT NULL,
    review_title character varying(255),
    review_content text,
    start_date timestamp without time zone NOT NULL,
    end_date timestamp without time zone DEFAULT '9999-12-31 00:00:00'::timestamp without time zone NOT NULL,
    source_id integer NOT NULL,
    insert_id integer,
    update_id integer
);


ALTER TABLE warehouse.reviews OWNER TO postgres;

--
-- TOC entry 234 (class 1259 OID 27057)
-- Name: reviews_reviews_sk_seq; Type: SEQUENCE; Schema: warehouse; Owner: postgres
--

CREATE SEQUENCE warehouse.reviews_reviews_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE warehouse.reviews_reviews_sk_seq OWNER TO postgres;

--
-- TOC entry 5077 (class 0 OID 0)
-- Dependencies: 234
-- Name: reviews_reviews_sk_seq; Type: SEQUENCE OWNED BY; Schema: warehouse; Owner: postgres
--

ALTER SEQUENCE warehouse.reviews_reviews_sk_seq OWNED BY warehouse.reviews.reviews_sk;


--
-- TOC entry 231 (class 1259 OID 27031)
-- Name: users; Type: TABLE; Schema: warehouse; Owner: postgres
--

CREATE TABLE warehouse.users (
    users_sk integer NOT NULL,
    user_id character varying(255) NOT NULL,
    user_name character varying(255) NOT NULL,
    start_date timestamp without time zone NOT NULL,
    end_date timestamp without time zone DEFAULT '9999-12-31 00:00:00'::timestamp without time zone NOT NULL,
    source_id integer NOT NULL,
    insert_id integer,
    update_id integer
);


ALTER TABLE warehouse.users OWNER TO postgres;

--
-- TOC entry 230 (class 1259 OID 27030)
-- Name: users_users_sk_seq; Type: SEQUENCE; Schema: warehouse; Owner: postgres
--

CREATE SEQUENCE warehouse.users_users_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE warehouse.users_users_sk_seq OWNER TO postgres;

--
-- TOC entry 5078 (class 0 OID 0)
-- Dependencies: 230
-- Name: users_users_sk_seq; Type: SEQUENCE OWNED BY; Schema: warehouse; Owner: postgres
--

ALTER SEQUENCE warehouse.users_users_sk_seq OWNED BY warehouse.users.users_sk;


--
-- TOC entry 4827 (class 2604 OID 25866)
-- Name: categories category_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.categories ALTER COLUMN category_id SET DEFAULT nextval('public.categories_category_id_seq'::regclass);


--
-- TOC entry 4828 (class 2604 OID 25911)
-- Name: locations location_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.locations ALTER COLUMN location_id SET DEFAULT nextval('public.locations_location_id_seq'::regclass);


--
-- TOC entry 4842 (class 2604 OID 27992)
-- Name: dim_category category_sk; Type: DEFAULT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_category ALTER COLUMN category_sk SET DEFAULT nextval('star.dim_category_category_sk_seq'::regclass);


--
-- TOC entry 4841 (class 2604 OID 27983)
-- Name: dim_date date_sk; Type: DEFAULT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_date ALTER COLUMN date_sk SET DEFAULT nextval('star.dim_date_date_sk_seq'::regclass);


--
-- TOC entry 4845 (class 2604 OID 28028)
-- Name: dim_location location_sk; Type: DEFAULT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_location ALTER COLUMN location_sk SET DEFAULT nextval('star.dim_location_location_sk_seq'::regclass);


--
-- TOC entry 4844 (class 2604 OID 28012)
-- Name: dim_product product_sk; Type: DEFAULT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_product ALTER COLUMN product_sk SET DEFAULT nextval('star.dim_product_product_sk_seq'::regclass);


--
-- TOC entry 4843 (class 2604 OID 28001)
-- Name: dim_user user_sk; Type: DEFAULT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_user ALTER COLUMN user_sk SET DEFAULT nextval('star.dim_user_user_sk_seq'::regclass);


--
-- TOC entry 4846 (class 2604 OID 28037)
-- Name: fact_pricing pricing_sk; Type: DEFAULT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.fact_pricing ALTER COLUMN pricing_sk SET DEFAULT nextval('star.fact_pricing_pricing_sk_seq'::regclass);


--
-- TOC entry 4829 (class 2604 OID 27025)
-- Name: categories categories_sk; Type: DEFAULT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.categories ALTER COLUMN categories_sk SET DEFAULT nextval('warehouse.categories_categories_sk_seq'::regclass);


--
-- TOC entry 4839 (class 2604 OID 27096)
-- Name: exchange_rates exchange_rates_sk; Type: DEFAULT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.exchange_rates ALTER COLUMN exchange_rates_sk SET DEFAULT nextval('warehouse.exchange_rates_exchange_rates_sk_seq'::regclass);


--
-- TOC entry 4837 (class 2604 OID 27082)
-- Name: locations locations_sk; Type: DEFAULT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.locations ALTER COLUMN locations_sk SET DEFAULT nextval('warehouse.locations_locations_sk_seq'::regclass);


--
-- TOC entry 4833 (class 2604 OID 27045)
-- Name: products products_sk; Type: DEFAULT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.products ALTER COLUMN products_sk SET DEFAULT nextval('warehouse.products_products_sk_seq'::regclass);


--
-- TOC entry 4835 (class 2604 OID 27061)
-- Name: reviews reviews_sk; Type: DEFAULT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.reviews ALTER COLUMN reviews_sk SET DEFAULT nextval('warehouse.reviews_reviews_sk_seq'::regclass);


--
-- TOC entry 4831 (class 2604 OID 27034)
-- Name: users users_sk; Type: DEFAULT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.users ALTER COLUMN users_sk SET DEFAULT nextval('warehouse.users_users_sk_seq'::regclass);


--
-- TOC entry 4848 (class 2606 OID 25870)
-- Name: categories categories_category_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.categories
    ADD CONSTRAINT categories_category_name_key UNIQUE (category_name);


--
-- TOC entry 4850 (class 2606 OID 25868)
-- Name: categories categories_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.categories
    ADD CONSTRAINT categories_pkey PRIMARY KEY (category_id);


--
-- TOC entry 4858 (class 2606 OID 25913)
-- Name: locations locations_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.locations
    ADD CONSTRAINT locations_pkey PRIMARY KEY (location_id);


--
-- TOC entry 4852 (class 2606 OID 25877)
-- Name: products products_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_pkey PRIMARY KEY (product_id);


--
-- TOC entry 4856 (class 2606 OID 25896)
-- Name: reviews reviews_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.reviews
    ADD CONSTRAINT reviews_pkey PRIMARY KEY (review_id);


--
-- TOC entry 4854 (class 2606 OID 25889)
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (user_id);


--
-- TOC entry 4883 (class 2606 OID 27996)
-- Name: dim_category dim_category_category_id_key; Type: CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_category
    ADD CONSTRAINT dim_category_category_id_key UNIQUE (category_id);


--
-- TOC entry 4885 (class 2606 OID 27994)
-- Name: dim_category dim_category_pkey; Type: CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_category
    ADD CONSTRAINT dim_category_pkey PRIMARY KEY (category_sk);


--
-- TOC entry 4879 (class 2606 OID 27987)
-- Name: dim_date dim_date_full_date_key; Type: CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_date
    ADD CONSTRAINT dim_date_full_date_key UNIQUE (full_date);


--
-- TOC entry 4881 (class 2606 OID 27985)
-- Name: dim_date dim_date_pkey; Type: CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_date
    ADD CONSTRAINT dim_date_pkey PRIMARY KEY (date_sk);


--
-- TOC entry 4895 (class 2606 OID 28032)
-- Name: dim_location dim_location_location_id_key; Type: CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_location
    ADD CONSTRAINT dim_location_location_id_key UNIQUE (location_id);


--
-- TOC entry 4897 (class 2606 OID 28030)
-- Name: dim_location dim_location_pkey; Type: CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_location
    ADD CONSTRAINT dim_location_pkey PRIMARY KEY (location_sk);


--
-- TOC entry 4891 (class 2606 OID 28016)
-- Name: dim_product dim_product_pkey; Type: CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_product
    ADD CONSTRAINT dim_product_pkey PRIMARY KEY (product_sk);


--
-- TOC entry 4893 (class 2606 OID 28018)
-- Name: dim_product dim_product_product_id_key; Type: CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_product
    ADD CONSTRAINT dim_product_product_id_key UNIQUE (product_id);


--
-- TOC entry 4887 (class 2606 OID 28005)
-- Name: dim_user dim_user_pkey; Type: CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_user
    ADD CONSTRAINT dim_user_pkey PRIMARY KEY (user_sk);


--
-- TOC entry 4889 (class 2606 OID 28007)
-- Name: dim_user dim_user_user_id_key; Type: CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_user
    ADD CONSTRAINT dim_user_user_id_key UNIQUE (user_id);


--
-- TOC entry 4899 (class 2606 OID 28039)
-- Name: fact_pricing fact_pricing_pkey; Type: CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.fact_pricing
    ADD CONSTRAINT fact_pricing_pkey PRIMARY KEY (pricing_sk);


--
-- TOC entry 4861 (class 2606 OID 27028)
-- Name: categories categories_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.categories
    ADD CONSTRAINT categories_pkey PRIMARY KEY (categories_sk);


--
-- TOC entry 4876 (class 2606 OID 27099)
-- Name: exchange_rates exchange_rates_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.exchange_rates
    ADD CONSTRAINT exchange_rates_pkey PRIMARY KEY (exchange_rates_sk);


--
-- TOC entry 4874 (class 2606 OID 27085)
-- Name: locations locations_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.locations
    ADD CONSTRAINT locations_pkey PRIMARY KEY (locations_sk);


--
-- TOC entry 4868 (class 2606 OID 27050)
-- Name: products products_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.products
    ADD CONSTRAINT products_pkey PRIMARY KEY (products_sk);


--
-- TOC entry 4871 (class 2606 OID 27066)
-- Name: reviews reviews_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.reviews
    ADD CONSTRAINT reviews_pkey PRIMARY KEY (reviews_sk);


--
-- TOC entry 4865 (class 2606 OID 27039)
-- Name: users users_pkey; Type: CONSTRAINT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (users_sk);


--
-- TOC entry 4859 (class 1259 OID 26180)
-- Name: idx_staging_fetched_at; Type: INDEX; Schema: staging; Owner: postgres
--

CREATE INDEX idx_staging_fetched_at ON staging.exchange_rates_raw USING btree (fetched_at);


--
-- TOC entry 4900 (class 1259 OID 28050)
-- Name: idx_fact_pricing_date; Type: INDEX; Schema: star; Owner: postgres
--

CREATE INDEX idx_fact_pricing_date ON star.fact_pricing USING btree (date_sk);


--
-- TOC entry 4901 (class 1259 OID 28051)
-- Name: idx_fact_pricing_product; Type: INDEX; Schema: star; Owner: postgres
--

CREATE INDEX idx_fact_pricing_product ON star.fact_pricing USING btree (product_sk);


--
-- TOC entry 4862 (class 1259 OID 27029)
-- Name: idx_categories_natural; Type: INDEX; Schema: warehouse; Owner: postgres
--

CREATE INDEX idx_categories_natural ON warehouse.categories USING btree (category_id);


--
-- TOC entry 4877 (class 1259 OID 27105)
-- Name: idx_exchange_rates_product; Type: INDEX; Schema: warehouse; Owner: postgres
--

CREATE INDEX idx_exchange_rates_product ON warehouse.exchange_rates USING btree (product_sk);


--
-- TOC entry 4872 (class 1259 OID 27086)
-- Name: idx_locations_natural; Type: INDEX; Schema: warehouse; Owner: postgres
--

CREATE INDEX idx_locations_natural ON warehouse.locations USING btree (location_id);


--
-- TOC entry 4866 (class 1259 OID 27051)
-- Name: idx_products_natural; Type: INDEX; Schema: warehouse; Owner: postgres
--

CREATE INDEX idx_products_natural ON warehouse.products USING btree (product_id);


--
-- TOC entry 4869 (class 1259 OID 27067)
-- Name: idx_reviews_natural; Type: INDEX; Schema: warehouse; Owner: postgres
--

CREATE INDEX idx_reviews_natural ON warehouse.reviews USING btree (review_id);


--
-- TOC entry 4863 (class 1259 OID 27040)
-- Name: idx_users_natural; Type: INDEX; Schema: warehouse; Owner: postgres
--

CREATE INDEX idx_users_natural ON warehouse.users USING btree (user_id);


--
-- TOC entry 4905 (class 2606 OID 25914)
-- Name: locations locations_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.locations
    ADD CONSTRAINT locations_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(product_id);


--
-- TOC entry 4902 (class 2606 OID 25878)
-- Name: products products_category_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_category_id_fkey FOREIGN KEY (category_id) REFERENCES public.categories(category_id);


--
-- TOC entry 4903 (class 2606 OID 25897)
-- Name: reviews reviews_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.reviews
    ADD CONSTRAINT reviews_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(product_id);


--
-- TOC entry 4904 (class 2606 OID 25902)
-- Name: reviews reviews_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.reviews
    ADD CONSTRAINT reviews_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(user_id);


--
-- TOC entry 4911 (class 2606 OID 28019)
-- Name: dim_product dim_product_category_sk_fkey; Type: FK CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.dim_product
    ADD CONSTRAINT dim_product_category_sk_fkey FOREIGN KEY (category_sk) REFERENCES star.dim_category(category_sk) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 4912 (class 2606 OID 28040)
-- Name: fact_pricing fact_pricing_date_sk_fkey; Type: FK CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.fact_pricing
    ADD CONSTRAINT fact_pricing_date_sk_fkey FOREIGN KEY (date_sk) REFERENCES star.dim_date(date_sk) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 4913 (class 2606 OID 28045)
-- Name: fact_pricing fact_pricing_product_sk_fkey; Type: FK CONSTRAINT; Schema: star; Owner: postgres
--

ALTER TABLE ONLY star.fact_pricing
    ADD CONSTRAINT fact_pricing_product_sk_fkey FOREIGN KEY (product_sk) REFERENCES star.dim_product(product_sk) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 4910 (class 2606 OID 27100)
-- Name: exchange_rates fk_exchangerates_product_sk; Type: FK CONSTRAINT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.exchange_rates
    ADD CONSTRAINT fk_exchangerates_product_sk FOREIGN KEY (product_sk) REFERENCES warehouse.products(products_sk) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 4909 (class 2606 OID 27087)
-- Name: locations fk_locations_product_sk; Type: FK CONSTRAINT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.locations
    ADD CONSTRAINT fk_locations_product_sk FOREIGN KEY (product_sk) REFERENCES warehouse.products(products_sk) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 4906 (class 2606 OID 27052)
-- Name: products fk_products_category_sk; Type: FK CONSTRAINT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.products
    ADD CONSTRAINT fk_products_category_sk FOREIGN KEY (category_sk) REFERENCES warehouse.categories(categories_sk) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 4907 (class 2606 OID 27068)
-- Name: reviews fk_reviews_product_sk; Type: FK CONSTRAINT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.reviews
    ADD CONSTRAINT fk_reviews_product_sk FOREIGN KEY (product_sk) REFERENCES warehouse.products(products_sk) ON UPDATE CASCADE ON DELETE RESTRICT;


--
-- TOC entry 4908 (class 2606 OID 27073)
-- Name: reviews fk_reviews_user_sk; Type: FK CONSTRAINT; Schema: warehouse; Owner: postgres
--

ALTER TABLE ONLY warehouse.reviews
    ADD CONSTRAINT fk_reviews_user_sk FOREIGN KEY (user_sk) REFERENCES warehouse.users(users_sk) ON UPDATE CASCADE ON DELETE RESTRICT;


-- Completed on 2025-06-08 20:21:02

--
-- PostgreSQL database dump complete
--

