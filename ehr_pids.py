import pandas as pd

## List of participants with any EHR data
def get_ehr_df(dataset, project):
    query = f"""
    SELECT
       DISTINCT person_id
    FROM `{dataset}.measurement` AS m
    LEFT JOIN `{dataset}.measurement_ext` AS mm ON m.measurement_id = mm.measurement_id
    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
    UNION DISTINCT
    SELECT
       DISTINCT person_id
    FROM `{dataset}.condition_occurrence` AS m
    LEFT JOIN `{dataset}.condition_occurrence_ext` AS mm ON m.condition_occurrence_id = mm.condition_occurrence_id
    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
    UNION DISTINCT
    SELECT
       DISTINCT person_id
    FROM `{dataset}.device_exposure` AS m
    LEFT JOIN `{dataset}.device_exposure_ext` AS mm ON m.device_exposure_id = mm.device_exposure_id
    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
    UNION DISTINCT
    SELECT
       DISTINCT person_id
    FROM `{dataset}.drug_exposure` AS m
    LEFT JOIN `{dataset}.drug_exposure_ext` AS mm ON m.drug_exposure_id = mm.drug_exposure_id
    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
    UNION DISTINCT
    SELECT
       DISTINCT person_id
    FROM `{dataset}.observation` AS m
    LEFT JOIN `{dataset}.observation_ext` AS mm ON m.observation_id = mm.observation_id
    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
    UNION DISTINCT
    SELECT
       DISTINCT person_id
    FROM `{dataset}.procedure_occurrence` AS m
    LEFT JOIN `{dataset}.procedure_occurrence_ext` AS mm ON m.procedure_occurrence_id = mm.procedure_occurrence_id
    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
    UNION DISTINCT
    SELECT
       DISTINCT person_id
    FROM `{dataset}.visit_occurrence` AS m
    LEFT JOIN `{dataset}.visit_occurrence_ext` AS mm ON m.visit_occurrence_id = mm.visit_occurrence_id
    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
    """

    ehr_df = pd.read_gbq(query, dialect = 'standard',  project_id=project)
    
    return ehr_df
