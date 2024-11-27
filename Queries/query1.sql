use iia_group2;
WITH AggregatedData AS (
    SELECT 
        gpm.state AS State,
        DATE_FORMAT(gpm.datee, '%Y-%m') AS Month,  -- Extracting Year-Month
        YEAR(giy.start_date) AS Year,            -- Extracting Year
        SUM(gpm.settlement_IGST) AS Monthly_IGST_Settlement
    FROM 
        GST_PerMonth_Info_NS gpm
    JOIN 
        GST_Info_PerYear_Data_NS giy
        ON gpm.state = giy.state
    WHERE 
        gpm.state = @state AND YEAR(giy.start_date) = @year
    GROUP BY 
        gpm.state, Month, Year
)
SELECT 
    State,
    MAX(Year) AS Year,
    CONCAT(
        'For ', State, ' in ', @year,
        ', the total IGST settlement was ₹', FORMAT(SUM(Monthly_IGST_Settlement), 2),
        '. The monthly average was ₹', FORMAT(AVG(Monthly_IGST_Settlement), 2),
        ', with a highest settlement of ₹', FORMAT(MAX(Monthly_IGST_Settlement), 2),
        ' and a lowest settlement of ₹', FORMAT(MIN(Monthly_IGST_Settlement), 2), '.'
    ) AS Summary
FROM 
    AggregatedData
GROUP BY 
    State;