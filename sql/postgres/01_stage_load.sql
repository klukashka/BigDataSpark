CREATE SCHEMA IF NOT EXISTS stage;

DROP TABLE IF EXISTS stage.mock_data;
CREATE TABLE stage.mock_data (
  id                      integer,
  customer_first_name     text,
  customer_last_name      text,
  customer_age            integer,
  customer_email          text,
  customer_country        text,
  customer_postal_code    text,
  customer_pet_type       text,
  customer_pet_name       text,
  customer_pet_breed      text,
  seller_first_name       text,
  seller_last_name        text,
  seller_email            text,
  seller_country          text,
  seller_postal_code      text,
  product_name            text,
  product_category        text,
  product_price           numeric(12,2),
  product_quantity        integer,
  sale_date               date,
  sale_customer_id        integer,
  sale_seller_id          integer,
  sale_product_id         integer,
  sale_quantity           integer,
  sale_total_price        numeric(12,2),
  store_name              text,
  store_location          text,
  store_city              text,
  store_state             text,
  store_country           text,
  store_phone             text,
  store_email             text,
  pet_category            text,
  product_weight          numeric(12,2),
  product_color           text,
  product_size            text,
  product_brand           text,
  product_material        text,
  product_description     text,
  product_rating          numeric(4,2),
  product_reviews         integer,
  product_release_date    date,
  product_expiry_date     date,
  supplier_name           text,
  supplier_contact        text,
  supplier_email          text,
  supplier_phone          text,
  supplier_address        text,
  supplier_city           text,
  supplier_country        text
);

SET datestyle = 'MDY';

COPY stage.mock_data FROM '/data/MOCK_DATA.csv' CSV HEADER;
COPY stage.mock_data FROM '/data/MOCK_DATA (1).csv' CSV HEADER;
COPY stage.mock_data FROM '/data/MOCK_DATA (2).csv' CSV HEADER;
COPY stage.mock_data FROM '/data/MOCK_DATA (3).csv' CSV HEADER;
COPY stage.mock_data FROM '/data/MOCK_DATA (4).csv' CSV HEADER;
COPY stage.mock_data FROM '/data/MOCK_DATA (5).csv' CSV HEADER;
COPY stage.mock_data FROM '/data/MOCK_DATA (6).csv' CSV HEADER;
COPY stage.mock_data FROM '/data/MOCK_DATA (7).csv' CSV HEADER;
COPY stage.mock_data FROM '/data/MOCK_DATA (8).csv' CSV HEADER;
COPY stage.mock_data FROM '/data/MOCK_DATA (9).csv' CSV HEADER;

DO $$
DECLARE
  n bigint;
BEGIN
  SELECT COUNT(*) INTO n FROM stage.mock_data;
  RAISE NOTICE 'stage.mock_data rowcount=% (expected 10000)', n;
END $$;
