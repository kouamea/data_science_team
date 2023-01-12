# Participants with any physical measurement data
"""
SELECT DISTINCT person_id
FROM `{dataset}.measurement` AS m
JOIN `{dataset}.measurement_ext` AS mm ON m.measurement_id = mm.measurement_id
LEFT JOIN `{dataset}.concept` AS c ON m.measurement_concept_id = c.concept_id
WHERE mm.src_id = 'PPI/PM'
AND m.visit_occurrence_id IS NOT NULL
"""
