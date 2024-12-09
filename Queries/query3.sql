use iia_group2;
WITH MonthlyAggregated AS (
    SELECT 
        gpm.state AS State,
        DATE_FORMAT(gpm.datee, '%Y-%m') AS Month,  -- Extracting Year-Month
        YEAR(gpm.datee) AS Year,
        SUM(gpm.settlement_IGST) AS Monthly_IGST_Settlement
    FROM 
        GST_PerMonth_Info_NS gpm
    WHERE 
        gpm.state = @state AND YEAR(gpm.datee) = @year
    GROUP BY 
        gpm.state, Month, Year
)
SELECT 
    CONCAT(
        'For ', State, ' in ', Year, 
        ', the IGST settlement for ', Month, ' was ₹', FORMAT(Monthly_IGST_Settlement, 2), 
        '. This represents a ',
        FORMAT(
            CASE 
                WHEN LAG(Monthly_IGST_Settlement) OVER (PARTITION BY State ORDER BY Year, Month) IS NULL THEN 0
                ELSE 
                    ((Monthly_IGST_Settlement - LAG(Monthly_IGST_Settlement) OVER (PARTITION BY State ORDER BY Year, Month)) / 
                    LAG(Monthly_IGST_Settlement) OVER (PARTITION BY State ORDER BY Year, Month)) * 100 
            END, 2
        ), 
        '% ',
        CASE 
            WHEN LAG(Monthly_IGST_Settlement) OVER (PARTITION BY State ORDER BY Year, Month) IS NULL THEN 'initial value.'
            WHEN (Monthly_IGST_Settlement - LAG(Monthly_IGST_Settlement) OVER (PARTITION BY State ORDER BY Year, Month)) > 0 THEN 'increase.'
            ELSE 'decrease.'
        END
    ) AS Response
FROM 
    MonthlyAggregated;