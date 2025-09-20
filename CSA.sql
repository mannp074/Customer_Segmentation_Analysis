-- ============================================================================
-- Olist E-Commerce Customer Segmentation Project
-- ============================================================================
-- Author      : [Your Name]
-- Date        : 2025-09-20
-- Description : End-to-end customer segmentation pipeline using MySQL 8.
--               Steps: Data ingestion, ETL, RFM modeling, segmentation,
--               BI-ready views, automation, and sample analysis.
-- ============================================================================
-- Notes:
-- 1. Update the file paths in LOAD DATA commands before running.
-- 2. Script is designed for MySQL 8+ with window functions support.
-- ============================================================================

-- ============================================================================
-- 1. DATABASE SETUP
-- ============================================================================
CREATE DATABASE IF NOT EXISTS olist;
USE olist;

-- ============================================================================
-- 2. SCHEMA CREATION & DATA LOADING
-- ============================================================================
-- Table: customers_raw
DROP TABLE IF EXISTS customers_raw;
CREATE TABLE customers_raw (
    customer_id VARCHAR(100),
    customer_unique_id VARCHAR(100),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);
LOAD DATA LOCAL INFILE 'C:/PATH/TO/YOUR/CSV/olist_customers_dataset.csv'
INTO TABLE customers_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n' IGNORE 1 LINES;

-- Table: orders_raw
DROP TABLE IF EXISTS orders_raw;
CREATE TABLE orders_raw (
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    order_status VARCHAR(50),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME
);
LOAD DATA LOCAL INFILE 'C:/PATH/TO/YOUR/CSV/olist_orders_dataset.csv'
INTO TABLE orders_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n' IGNORE 1 LINES;

-- Table: order_items_raw
DROP TABLE IF EXISTS order_items_raw;
CREATE TABLE order_items_raw (
    order_id VARCHAR(100),
    order_item_id INT,
    product_id VARCHAR(100),
    seller_id VARCHAR(100),
    shipping_limit_date DATETIME,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2)
);
LOAD DATA LOCAL INFILE 'C:/PATH/TO/YOUR/CSV/olist_order_items_dataset.csv'
INTO TABLE order_items_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n' IGNORE 1 LINES;

-- Table: order_payments_raw
DROP TABLE IF EXISTS order_payments_raw;
CREATE TABLE order_payments_raw (
    order_id VARCHAR(100),
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10,2)
);
LOAD DATA LOCAL INFILE 'C:/PATH/TO/YOUR/CSV/olist_order_payments_dataset.csv'
INTO TABLE order_payments_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n' IGNORE 1 LINES;

-- Table: products_raw
DROP TABLE IF EXISTS products_raw;
CREATE TABLE products_raw (
    product_id VARCHAR(100),
    product_category_name VARCHAR(255),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT
);
LOAD DATA LOCAL INFILE 'C:/PATH/TO/YOUR/CSV/olist_products_dataset.csv'
INTO TABLE products_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n' IGNORE 1 LINES;

-- ============================================================================
-- 3. ETL & DATA MODELING PIPELINE
-- ============================================================================

-- 3.1: Create Canonical Order Values Table
DROP TABLE IF EXISTS order_values;
CREATE TABLE order_values AS
WITH order_items_agg AS (
    SELECT order_id, SUM(price + COALESCE(freight_value, 0)) AS items_total
    FROM order_items_raw
    GROUP BY order_id
),
order_payments_agg AS (
    SELECT order_id, SUM(payment_value) AS payments_total
    FROM order_payments_raw
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp AS order_date,
    oi.items_total,
    pay.payments_total
FROM orders_raw o
LEFT JOIN order_items_agg oi ON o.order_id = oi.order_id
LEFT JOIN order_payments_agg pay ON o.order_id = pay.order_id
WHERE o.order_status IN ('delivered', 'shipped');

-- 3.2: Build Customer RFM Table
DROP TABLE IF EXISTS customer_rfm;
CREATE TABLE customer_rfm AS
WITH cust_orders AS (
    SELECT
        c.customer_unique_id,
        ov.order_date,
        ov.order_id,
        COALESCE(ov.payments_total, ov.items_total, 0) AS order_value
    FROM customers_raw c
    JOIN order_values ov ON c.customer_id = ov.customer_id
)
SELECT
    customer_unique_id,
    MIN(order_date) AS first_purchase_date,
    MAX(order_date) AS last_purchase_date,
    DATEDIFF((SELECT MAX(order_date) FROM cust_orders), MAX(order_date)) AS recency_days,
    COUNT(DISTINCT order_id) AS frequency,
    SUM(order_value) AS monetary
FROM cust_orders
GROUP BY customer_unique_id;

-- 3.3: Score RFM Values
DROP TABLE IF EXISTS customer_rfm_scored;
CREATE TABLE customer_rfm_scored AS
WITH base AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days ASC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC, monetary DESC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC) AS m_score
    FROM customer_rfm
)
SELECT *,
    CONCAT(r_score, f_score, m_score) AS rfm_code
FROM base;

-- 3.4: Create Final Segments Table
DROP TABLE IF EXISTS customer_segments;
CREATE TABLE customer_segments AS
SELECT *,
    CASE
        WHEN rfm_code = '111' THEN 'Champions'
        WHEN f_score <= 2 AND r_score <= 2 THEN 'Loyal Customers'
        WHEN r_score = 1 AND frequency = 1 THEN 'New Customers'
        WHEN r_score <= 3 AND f_score >= 3 THEN 'Potential Loyalists'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'At-Risk Customers'
        WHEN r_score = 5 AND f_score >= 4 THEN 'Lost Customers'
        ELSE 'Other'
    END AS segment_name
FROM customer_rfm_scored;

-- ============================================================================
-- 4. DEPLOYMENT ASSETS
-- ============================================================================

-- View for BI: Final Customer Segments
CREATE OR REPLACE VIEW vw_customer_segments AS
SELECT
    customer_unique_id,
    segment_name,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    rfm_code
FROM customer_segments;

-- Stored Procedure to Refresh Pipeline
DROP PROCEDURE IF EXISTS refresh_customer_segments;
DELIMITER //
CREATE PROCEDURE refresh_customer_segments()
BEGIN
    -- Re-run ETL and segmentation steps here
    -- Replace the placeholders below with full queries from steps 3.1–3.4
    DROP TABLE IF EXISTS order_values;
    CREATE TABLE order_values AS SELECT ... ;

    DROP TABLE IF EXISTS customer_rfm;
    CREATE TABLE customer_rfm AS SELECT ... ;

    DROP TABLE IF EXISTS customer_rfm_scored;
    CREATE TABLE customer_rfm_scored AS SELECT ... ;

    DROP TABLE IF EXISTS customer_segments;
    CREATE TABLE customer_segments AS SELECT ... ;
END;
//
DELIMITER ;

-- ============================================================================
-- 5. SAMPLE ANALYSIS & VERIFICATION
-- ============================================================================

-- 5.1: Segment Business Summary
SELECT
    segment_name,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customer_segments), 2) AS pct_of_total_customers,
    ROUND(AVG(monetary), 2) AS avg_lifetime_spend,
    ROUND(AVG(recency_days), 0) AS avg_recency_days
FROM customer_segments
GROUP BY segment_name
ORDER BY customer_count DESC;

-- 5.2: Target List for At-Risk Customers Campaign
SELECT
    customer_unique_id,
    last_purchase_date,
    recency_days,
    frequency,
    monetary
FROM customer_segments
WHERE segment_name = 'At-Risk Customers'
ORDER BY monetary DESC
LIMIT 500;

-- 5.3: Champions Performance Check
SELECT
    customer_unique_id,
    recency_days,
    frequency,
    monetary,
    rfm_code
FROM customer_segments
WHERE segment_name = 'Champions'
ORDER BY monetary DESC
LIMIT 100;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
