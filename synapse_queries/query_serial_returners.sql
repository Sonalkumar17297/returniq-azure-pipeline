-- Query 1: Top Serial Returners
SELECT 
    User_ID,
    Total_Orders,
    Total_Returns,
    Return_Rate,
    Is_Serial_Returner
FROM OPENROWSET(
    BULK 'https://returniqadls.dfs.core.windows.net/gold/user_return_rates/**',
    FORMAT = 'PARQUET'
) AS result
WHERE Is_Serial_Returner = 1
ORDER BY Return_Rate DESC