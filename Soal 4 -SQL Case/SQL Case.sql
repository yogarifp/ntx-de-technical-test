{# Test Case 1: Channel Analysis #}
WITH country_revenue AS (
    SELECT 
        country, 
        SUM(totalTransactionRevenue) AS total_revenue
    FROM ecommerce_sessions
    WHERE totalTransactionRevenue IS NOT NULL
    GROUP BY country
    ORDER BY total_revenue DESC
    LIMIT 5
) --CTE country_revenue: Calculates the total transaction revenue per country and selects the top 5 countries with the highest revenue. This helps in narrowing down the focus to only the most lucrative countries.

SELECT 
    es.channelGrouping, 
    es.country, 
    SUM(es.totalTransactionRevenue) AS total_revenue
FROM ecommerce_sessions es
JOIN country_revenue cr
ON es.country = cr.country
WHERE es.totalTransactionRevenue IS NOT NULL
GROUP BY es.channelGrouping, es.country
ORDER BY total_revenue DESC;
-- Final SELECT: Joins the ecommerce_sessions table with the country_revenue CTE to filter the sessions to only those from the top 5 countries. It then aggregates the revenue per channelGrouping for these countries.

{# Test Case 2: User Behavior Analysis #}
WITH user_metrics AS (
    SELECT 
        fullVisitorId, 
        AVG(timeOnSite) AS avg_timeOnSite, 
        AVG(pageviews) AS avg_pageviews, 
        COALESCE(AVG(sessionQualityDim), 0) AS avg_sessionQuality
    FROM ecommerce_sessions
    GROUP BY fullVisitorId
) --CTE user_metrics to calculate average metrics for each fullVisitorId.
, overall_avg AS (
    SELECT 
        AVG(timeOnSite) AS overall_avg_timeOnSite, 
        AVG(pageviews) AS overall_avg_pageviews
    FROM ecommerce_sessions
) --CTE overall_avg to compute overall averages for time on site and pageviews.

SELECT 
    um.fullVisitorId, 
    um.avg_timeOnSite, 
    um.avg_pageviews, 
    um.avg_sessionQuality
FROM user_metrics um, overall_avg oa
WHERE um.avg_timeOnSite > oa.overall_avg_timeOnSite 
AND um.avg_pageviews < oa.overall_avg_pageviews;
-- Selects users who have above-average time on site but below-average pageviews.

{# Test Case 3: Product Performance #}
WITH product_stats AS (
    SELECT 
        v2ProductName, 
        COALESCE(SUM(totalTransactionRevenue), 0) AS total_revenue, 
        COALESCE(SUM(productQuantity), 0) AS total_quantity, 
        COALESCE(SUM(productRefundAmount), 0) AS total_refunds
    FROM ecommerce_sessions
    GROUP BY v2ProductName
) --CTE product_stats to calculate total revenue, quantity sold, and refunds for each product.
, product_net_revenue AS (
    SELECT 
        v2ProductName, 
        total_revenue, 
        total_quantity, 
        total_refunds, 
        (total_revenue - total_refunds) AS net_revenue,
        CASE 
            WHEN total_revenue > 0 AND total_refunds > 0.1 * total_revenue THEN 'Refund > 10%' 
            ELSE 'Normal' 
        END AS refund_flag
    FROM product_stats
) --CTE product_net_revenue to compute net revenue and flag products where refunds exceed 10% of total revenue.

SELECT 
    v2ProductName, 
    total_revenue, 
    total_quantity, 
    total_refunds, 
    net_revenue, 
    refund_flag
FROM product_net_revenue
ORDER BY net_revenue DESC;



/*
Brief Report

Data Understanding:
1. The dataset contains e-commerce session information, including channel grouping, country, visit metrics, and product statistics.
2. Several key columns have NULL values (all value in productRevenue is NULL), necessitating the use of COALESCE to ensure accurate calculations.

Data Quality Issues:
Many important columns have NULL values, which were handled by substituting 0 to maintain accuracy in calculations, particularly for refunds and revenue.

Insights Derived:
1. Channel Analysis: Identifies how different channels contribute to revenue in the top 5 revenue-generating countries.
2. User Behavior Analysis: Highlights users with high engagement but low pageviews, potentially indicating unique browsing patterns.
3. Product Performance: Reveals products with high refund rates, which could be prioritized for further review or improvement.
*/
