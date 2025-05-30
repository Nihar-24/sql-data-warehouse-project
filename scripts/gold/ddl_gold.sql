/*
===========================================================================================================
DDL Script: Create Gold Views
===========================================================================================================
Script Purpose:
This script creates views for the Gold layer in the data warehouse.
The Gold layer represents the final dimension and fact tables (Star Schema)

Each view performs transformations and combines data from the Silver layer
to produce a clean, enriched, and business-ready dataset.

Usaage:
These views can be queried directly for analytics and reporting.
*/
-- ========================================================================================================
-- Create Dimension: gold.dim_customers
-- ========================================================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
DROP VIEW gold.dim_customers
GO
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	loc.cntry AS country,
	CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr --CRM is the master for gender info
	ELSE COALESCE(eci.gen, 'N/A')
	END AS gender,
	ci.cst_marital_status AS martital_status,
	eci.bdate AS birthdate,
	ci.cst_create_date AS create_date
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 AS eci
	ON		  ci.cst_key = eci.cid
	LEFT JOIN silver.erp_loc_a101 AS loc
	ON		  ci.cst_key = loc.cid
-- ========================================================================================================
-- Create Dimension: gold.dim_products
-- ========================================================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
DROP VIEW gold.dim_products
GO
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER () OVER (ORDER BY prd_start_dt, prd_key) AS product_key,
	cp.prd_id AS product_id,
	cp.prd_key AS product_number,
	cp.prd_line AS product_line,
	cp.prd_nm AS product_name,
	cp.cat_id AS category_id,
	ep.cat AS category,
	ep.subcat AS subcategory,
	ep.maintenance,
	cp.prd_cost AS product_cost,
	cp.prd_start_dt AS start_date
FROM silver.crm_prd_info AS cp
LEFT JOIN silver.erp_px_cat_g1v2 AS ep
ON cp.cat_id = ep.id
WHERE cp.prd_end_dt IS NULL -- Filter out historical data
-- ========================================================================================================
-- Create Fact: gold.fact_sales
-- ========================================================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
DROP VIEW gold.fact_sales
GO
CREATE VIEW gold.fact_sales AS
SELECT
  sd.sls_ord_num AS order_number,
  pr.product_key,
  cu.customer_key,
  sd.sls_order_dt AS order_date,
  sd.sls_ship_dt AS shipping_date,
  sd.sls_due_dt AS due_date,
  sd.sls_sales AS sales,
  sd.sls_quantity AS quantity,
  sd.sls_price AS price
  FROM silver.crm_sales_details AS sd
  LEFT JOIN gold.dim_products AS pr
  ON sd.sls_prd_key = pr.product_number
  LEFT JOIN gold.dim_customers AS cu
  ON sd.sls_cust_id = cu.customer_id
