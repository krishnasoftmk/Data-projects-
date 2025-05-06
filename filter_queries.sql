-- SQL Query 1: Get the top 10 most common model_year, make, model based on distinct Sent_VIN
SELECT 
    Model_Year, 
    Make, 
    Model, 
    COUNT(DISTINCT Sent_VIN) AS total_count
FROM `sahayaproject.nhtsa_dataset.processed_nhtsa_data`
GROUP BY Model_Year, Make, Model
ORDER BY total_count DESC
LIMIT 10;

-- SQL Query 2: Count distinct Sent_VIN grouped by LX_BodyClass_lvl1
SELECT 
    LX_BodyClass_lvl1 AS bodysegment,
    COUNT(DISTINCT p.Sent_VIN) AS total_count
FROM `sahayaproject.nhtsa_dataset.processed_nhtsa_data` p
JOIN `sahayaproject.nhtsa_dataset.nhtsa_lookup_table` l
ON p.Vehicle_Type_ID = l.Vehicle_Type_ID 
AND p.Body_Class_ID = l.Body_Class_ID
WHERE LX_BodyClass_lvl1 NOT IN ('MOTORCYCLE', 'BUS')
AND NOT (LX_BodyClass_lvl1 = 'PASSENGER CAR' AND LX_BodyClass_lvl2 = 'CONVERTIBLE')
GROUP BY bodysegment
ORDER BY total_count DESC;
