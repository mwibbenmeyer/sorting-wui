-- filename: create_and_load_lar_2007.sql

DROP TABLE IF EXISTS hmda_public.lar_2002;
CREATE TABLE hmda_public.lar_2002(   
    activity_year VARCHAR,
    respondent_id VARCHAR,
    agency_code VARCHAR,
    loan_type VARCHAR,
    loan_purpose VARCHAR,
    occupancy VARCHAR,
    loan_amount VARCHAR,
    action_type VARCHAR,
    msa VARCHAR,
    state_code VARCHAR,
    county_code VARCHAR,
    census_tract VARCHAR,
    applicant_race VARCHAR,
    co_applicant_race VARCHAR,
    applicant_sex VARCHAR,
    co_applicant_sex VARCHAR,
    income VARCHAR,
    purchaser_type VARCHAR,
    denial_1 VARCHAR,
    denial_2 VARCHAR,
    denial_3 VARCHAR,
    edit_status VARCHAR,
    sequence_num VARCHAR
    );

COMMIT;

CREATE TEMPORARY TABLE lar_load
(LAR VARCHAR); -- LAR contains an entire LAR record

COPY lar_load
-- Change this path to your local data path.
FROM '/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/Raw_data/HMDA/data/lar/lar_2002.dat';

COMMIT;

INSERT INTO hmda_public.lar_2002 (
    activity_year,
    respondent_id,
    agency_code,
    loan_type,
    loan_purpose,
    occupancy,
    loan_amount,
    action_type,
    msa,
    state_code,
    county_code,
    census_tract,
    applicant_race,
    co_applicant_race,
    applicant_sex,
    co_applicant_sex,
    income,
    purchaser_type,
    denial_1,
    denial_2,
    denial_3,
    edit_status,
    sequence_num
    )

SELECT 
SUBSTRING(LAR, 1,4),
SUBSTRING(LAR, 5,10),
SUBSTRING(LAR, 15,1),
SUBSTRING(LAR, 16,1),
SUBSTRING(LAR, 17,1),
SUBSTRING(LAR, 18,1),
SUBSTRING(LAR, 19,5),
SUBSTRING(LAR, 24,1),
SUBSTRING(LAR, 25,4),
SUBSTRING(LAR, 29,2),
SUBSTRING(LAR, 31,3),
SUBSTRING(LAR, 34,7),
SUBSTRING(LAR, 41,1),
SUBSTRING(LAR, 42,1),
SUBSTRING(LAR, 43,4),
SUBSTRING(LAR, 44,1),
SUBSTRING(LAR, 45,4),
SUBSTRING(LAR, 49,1),
SUBSTRING(LAR, 50,1),
SUBSTRING(LAR, 51,1),
SUBSTRING(LAR, 52,1),
SUBSTRING(LAR, 53,1),
SUBSTRING(LAR, 54,7)

FROM lar_load;
COMMIT;
DROP TABLE IF EXISTS lar_load; 
COMMIT;