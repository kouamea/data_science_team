import pandas as pd
import os

dataset = os.getenv('WORKSPACE_CDR') 
## List of participants with any EHR data
def ehr_cohort(dataset = dataset):
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

    ehr_df = pd.read_gbq(query, dialect = 'standard')
    
    return ehr_df


def physical_measurement_cohort(dataset = dataset):
    query = f"""
    SELECT
       DISTINCT person_id
    FROM `{dataset}.measurement` AS m
    INNER JOIN `{dataset}.measurement_ext` AS mm ON m.measurement_id = mm.measurement_id
    LEFT JOIN `{dataset}.concept` AS c ON m.measurement_concept_id = c.concept_id
    WHERE mm.src_id = 'PPI/PM'
    AND m.visit_occurrence_id IS NOT NULL
    """
    pm_df = pd.read_gbq(query, dialect = 'standard')
    
    return pm_df


def fitbit_cohort(dataset = dataset):
    query = f"""
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
        """
    fitbit_df = pd.read_gbq(query, dialect = 'standard')
    return fitbit_df

def cope_cohort(dataset = dataset):
    query = f"""
          SELECT DISTINCT person_id
        FROM `{dataset}.observation`
        INNER JOIN `{dataset}.observation_ext` USING(observation_id)
        INNER JOIN `{dataset}.concept` v ON v.concept_id=survey_version_concept_id
        WHERE lower(v.concept_name) LIKE '%cope survey%'
        """
    cope_df = pd.read_gbq(query, dialect = 'standard',  project_id=project)
    return cope_df
