-- Query 2: Category Return Rates
SELECT 
    Product_Category,
    Total_Orders,
    Total_Returns,
    Category_Return_Rate
FROM OPENROWSET(
    BULK 'https://returniqadls.dfs.core.windows.net/gold/category_return_rates/**',
    FORMAT = 'PARQUET'
) AS result
ORDER BY Category_Return_Rate DESC