"""
SELECT 
  DISTINCT person_id 
  FROM `{dataset}.heart_rate_minute_level`
UNION DISTINCT
SELECT 
  DISTINCT person_id 
  FROM `{dataset}.heart_rate_summary`
UNION DISTINCT
SELECT 
  DISTINCT person_id 
  FROM `{dataset}.steps_intraday`
UNION DISTINCT
SELECT 
  DISTINCT person_id 
  FROM `{dataset}.activity_summary`          
UNION DISTINCT
SELECT 
  DISTINCT person_id
  FROM `{dataset}.sleep_daily_summary`
UNION DISTINCT
SELECT 
  DISTINCT person_id
  FROM `{dataset}.sleep_level`
"""
