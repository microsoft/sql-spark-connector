EXEC sp_cleanuptables 'all'

--Ensure the counts are 0 for both tables below
-- ********************************************
SELECT count(*)
FROM [dbo].[StagingPredictedCovid19]

SELECT count(*)
FROM [dbo].[FactCovid19]

-- ********************************************