
-- Total Sales
SELECT
	ROUND(SUM(sales), 2) AS total_sales
FROM
	orders;

-- Total Profit
SELECT
	ROUND(SUM(profit), 2) AS total_profit
FROM
	orders;

-- Profit Ratio (Margin)
SELECT
	ROUND(SUM(profit)/SUM(sales), 2) AS profit_ratio_abs
	, ROUND(SUM(profit)/SUM(sales), 2) * 100 AS profit_ratio_pct
FROM
	orders;

-- Profit per Order
SELECT
	order_id,
	sum(profit) AS profit_per_order
FROM
	orders
GROUP BY order_id;

-- Sales per Customer
SELECT
	customer_id,
	sum(sales) AS sales_per_customer
FROM
	orders
GROUP BY customer_id;


-- Avg. Discount
SELECT
	ROUND(AVG(discount), 2) AS avg_discount_abs
	, ROUND(AVG(discount), 2) * 100 AS avg_discount_pct
FROM
	orders;

-- Monthly Sales by Segment
SELECT
	segment
	, DATE_TRUNC('month', order_date)::DATE AS "month" 
	, SUM(sales) AS total_sales
FROM
	orders
GROUP BY
	segment,
	DATE_TRUNC('month', order_date)::DATE
ORDER BY
	segment,
	"month";

-- Monthly Sales by Product Category
SELECT
	category AS product_category
	, DATE_TRUNC('month', order_date)::DATE AS "month" 
	, SUM(sales) AS total_sales
FROM
	orders
GROUP BY
	category,
	DATE_TRUNC('month', order_date)::DATE
ORDER BY
	category,
	"month";
	
--Sales and Profit by Customer
--Customer Ranking
SELECT
	customer_id
	, SUM(sales) AS total_sales
	, SUM(profit) AS total_profit
	, RANK() OVER(ORDER BY SUM(profit) DESC, SUM(sales) DESC) AS customer_rank
FROM
	orders
GROUP BY
	customer_id;

--Sales per region
SELECT
	region
	, ROUND(SUM(sales), 2) AS total_sales
FROM
	orders
GROUP BY
	region
ORDER BY SUM(sales) DESC;