use iia_group2;
WITH AggregatedData AS (
    SELECT 
        gpp.state AS State,
        SUM(IFNULL(gpp.registered_payers, 0)) AS Total_Registered_Payers,
        SUM(IFNULL(giy.UIN_holders, 0)) AS Total_UIN_Holders,
        SUM(IFNULL(gpp.eligible_payers, 0)) AS Total_Eligible_Payers
    FROM 
        GST_Payers_Info_PerMonth_NS gpp
    JOIN 
        GST_Info_PerYear_Data_NS giy
        ON gpp.state = giy.state
    GROUP BY 
        gpp.state
)
SELECT 
    State,
    CONCAT(
        'In ', State, 
        ', there were a total of ', FORMAT(Total_Registered_Payers, 0), ' registered payers, and ',
        FORMAT(Total_UIN_Holders, 0), ' UIN holders across the data sources.'
    ) AS Summary
FROM 
    AggregatedData
ORDER BY 
    Total_Registered_Payers DESC
LIMIT @top_n;