# age at primary consent
'''
SELECT COUNT(DISTINCT a.person_id) as n
            , FLOOR(DATE_DIFF(DATE(coalesce(e.observation_date, s.observation_date, b.observation_date))
                    , DATE(birth_datetime), DAY)/365.25) as age_at_consent
            
FROM `{dataset}.person` a
LEFT JOIN `{dataset}.observation` e 
     ON a.person_id = e.person_id
     AND (e.observation_source_concept_id = 1585482 OR e.observation_concept_id = 1585482) #extra consent date
LEFT JOIN `{dataset}.observation` s 
     ON a.person_id = s.person_id
     AND s.observation_source_concept_id = 1585249 # pii state date
LEFT JOIN (SELECT person_id, MIN(observation_date) AS observation_date
           FROM `{dataset}.observation` b 
           JOIN `{dataset}.concept_ancestor` on descendant_concept_id=observation_concept_id
           WHERE ancestor_concept_id = 1586134 GROUP BY 1) b # the basics date
      ON a.person_id = b.person_id
                
GROUP BY 2
ORDER BY 2,1
'''
