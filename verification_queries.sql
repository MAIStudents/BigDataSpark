-- ================================================
-- SQL запросы для проверки данных в PostgreSQL
-- ================================================

-- 1. Проверка исходных данных
SELECT COUNT(*) as total_records FROM mock_data;
-- Ожидается: 10000 записей

-- 2. Проверка таблиц измерений
SELECT 'dim_customers' as table_name, COUNT(*) as count FROM dim_customers
UNION ALL
SELECT 'dim_sellers', COUNT(*) FROM dim_sellers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'dim_stores', COUNT(*) FROM dim_stores
UNION ALL
SELECT 'dim_suppliers', COUNT(*) FROM dim_suppliers
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales;

-- 3. Топ-10 продуктов по выручке
SELECT 
    p.name,
    p.category,
    SUM(f.total_price) as total_revenue,
    SUM(f.quantity) as total_quantity
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.name, p.category
ORDER BY total_revenue DESC
LIMIT 10;

-- 4. Топ-10 клиентов по сумме покупок
SELECT 
    c.first_name || ' ' || c.last_name as customer_name,
    c.country,
    SUM(f.total_price) as total_spent,
    COUNT(f.sale_id) as purchase_count
FROM fact_sales f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.country
ORDER BY total_spent DESC
LIMIT 10;

-- 5. Продажи по месяцам
SELECT 
    t.year,
    t.month,
    COUNT(f.sale_id) as sales_count,
    SUM(f.total_price) as total_revenue,
    AVG(f.total_price) as avg_order_size
FROM fact_sales f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY t.year, t.month
ORDER BY t.year, t.month;

-- 6. Топ-5 магазинов по выручке
SELECT 
    s.name,
    s.city,
    s.country,
    SUM(f.total_price) as total_revenue,
    COUNT(f.sale_id) as sales_count
FROM fact_sales f
JOIN dim_stores s ON f.store_id = s.store_id
GROUP BY s.store_id, s.name, s.city, s.country
ORDER BY total_revenue DESC
LIMIT 5;

-- 7. Топ-5 поставщиков по выручке
SELECT 
    sup.name,
    sup.country,
    SUM(f.total_price) as total_revenue,
    COUNT(DISTINCT f.product_id) as products_count
FROM fact_sales f
JOIN dim_suppliers sup ON f.supplier_id = sup.supplier_id
GROUP BY sup.supplier_id, sup.name, sup.country
ORDER BY total_revenue DESC
LIMIT 5;

-- 8. Продукты с наивысшим рейтингом
SELECT 
    p.name,
    p.category,
    p.rating,
    p.reviews,
    SUM(f.quantity) as total_sold
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_id, p.name, p.category, p.rating, p.reviews
ORDER BY p.rating DESC, p.reviews DESC
LIMIT 10;

-- 9. Распределение клиентов по странам
SELECT 
    country,
    COUNT(*) as customer_count,
    SUM(total_spent) as total_revenue
FROM (
    SELECT 
        c.country,
        SUM(f.total_price) as total_spent
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    GROUP BY c.customer_id, c.country
) as customer_stats
GROUP BY country
ORDER BY total_revenue DESC;

-- 10. Средний чек по категориям продуктов
SELECT 
    p.category,
    COUNT(f.sale_id) as sales_count,
    SUM(f.total_price) as total_revenue,
    AVG(f.total_price) as avg_check
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;
