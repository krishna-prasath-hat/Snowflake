-- Example: Find rows where a keyword is present in a JSON column
SELECT *
FROM TEST_DB.TEST_SCHEMA.SAMPLE_PATIENT_DATA
WHERE ARRAY_CONTAINS(TO_VARIANT(case_type), 'in');


SELECT LEVENSHTEIN('hello', 'hullo') AS distance;


SELECT *
FROM TEST_DB.TEST_SCHEMA.SAMPLE_PATIENT_DATA
WHERE case_type LIKE 'in%';  


SELECT *
FROM TEST_DB.TEST_SCHEMA.SAMPLE_PATIENT_DATA
WHERE case_type ILIKE 'T%';  


SELECT *
FROM TEST_DB.TEST_SCHEMA.SAMPLE_PATIENT_DATA
WHERE REGEXP_LIKE(case_type, '^in.*$'); 





WITH data AS (
  SELECT ARRAY_CONSTRUCT('apple', 'orange', 'banana', 'grape') AS fruits
)
SELECT
  fruits
FROM data,
  LATERAL FLATTEN(input => fruits) AS flattened_fruits
WHERE value::STRING ILIKE '%ap%'; 



SELECT ST_Distance(
    ST_POINT(-122.4194, 37.7749),  -- San Francisco coordinates
    ST_POINT(-118.2437, 34.0522)   -- Los Angeles coordinates
) AS distance;  



SELECT a.patient_id, a.case_type, b.case_outcome AS similar_name
FROM TEST_DB.TEST_SCHEMA.SAMPLE_PATIENT_DATA a
JOIN TEST_DB.TEST_SCHEMA.SAMPLE_PATIENT_DATA b
ON a.case_type ILIKE b.case_outcome || '%'
WHERE a.patient_id <> b.patient_id;




SELECT 
    -- case_type, case_id,
    EDITDISTANCE('pap', 'pap') AS edit_distance,
    JAROWINKLER_SIMILARITY('pap', 'pap') AS similarity_score;
FROM 
    TEST_DB.TEST_SCHEMA.SAMPLE_PATIENT_DATA 
WHERE 
    --EDITDISTANCE(case_type, case_id) < 10  OR 
    JAROWINKLER_SIMILARITY(case_type, case_id) < 80;  

    SELECT JAROWINKLER_SIMILARITY('hello', 'llo') AS similarity;  -- Output: Value close to 0.9



select * from TEST_DB.TEST_SCHEMA.SAMPLE_PATIENT_DATA limit 10;



WITH split_data AS (
    SELECT
        CASE_ORIGIN,
        CASE_OUTCOME,
        TRIM(case_outcome) AS word_a
    FROM
        sample_patient_data,
        LATERAL FLATTEN(INPUT => SPLIT(CASE_ORIGIN, ' ')) AS word_a
),
split_data_b AS (
    SELECT
        CASE_ORIGIN,
        CASE_OUTCOME,
        TRIM(case_outcome) AS word_b
    FROM
        sample_patient_data,
        LATERAL FLATTEN(INPUT => SPLIT(CASE_OUTCOME, ' ')) AS word_b
),
word_counts AS (
    SELECT
        s1.CASE_ORIGIN,
        s1.CASE_OUTCOME,
        COUNT(DISTINCT s1.word_a) AS count_a,
        COUNT(DISTINCT s2.word_b) AS count_b
    FROM
        split_data s1
    LEFT JOIN
        split_data_b s2
    ON
        s1.CASE_ORIGIN = s2.CASE_ORIGIN AND s1.CASE_OUTCOME = s2.CASE_OUTCOME
    GROUP BY
        s1.CASE_ORIGIN, s1.CASE_OUTCOME
),
intersection_count AS (
    SELECT
        s1.CASE_ORIGIN,
        s1.CASE_OUTCOME,
        COUNT(DISTINCT s1.word_a) AS intersection_count
    FROM
        split_data s1
    JOIN
        split_data_b s2
    ON
        s1.CASE_ORIGIN = s2.CASE_ORIGIN AND s1.CASE_OUTCOME = s2.CASE_OUTCOME AND s1.word_a = s2.word_b
    GROUP BY
        s1.CASE_ORIGIN, s1.CASE_OUTCOME
),
union_count AS (
    SELECT
        wc.CASE_ORIGIN,
        wc.CASE_OUTCOME,
        (wc.count_a + wc.count_b - ic.intersection_count) AS union_count,
        ic.intersection_count
    FROM
        word_counts wc
    JOIN
        intersection_count ic
    ON
        wc.CASE_ORIGIN = ic.CASE_ORIGIN AND wc.CASE_OUTCOME = ic.CASE_OUTCOME
)
SELECT
    uc.CASE_ORIGIN,
    uc.CASE_OUTCOME,
    (uc.intersection_count * 1.0 / uc.union_count) AS jaccard_index
FROM
    union_count uc
WHERE
    uc.union_count > 0 AND
    (uc.intersection_count * 1.0 / uc.union_count) > 0.5;




SELECT 
    case_id,
    SOUNDEX(case_id) AS soundex_a,
FROM 
    TEST_DB.TEST_SCHEMA.SAMPLE_PATIENT_DATA
WHERE 
    SOUNDEX(case_id) = SOUNDEX('pap'); 



    SELECT
    -- SOUNDEX('Smith') AS soundex_smith,
    SOUNDEX('Smythe') AS soundex_smythe;






CREATE OR REPLACE FUNCTION HAMMING_DISTANCE(string_a STRING, string_b STRING)
RETURNS String
LANGUAGE JAVASCRIPT
AS
$$
    if (string_a.length !== string_b.length) {
        return null;  // Hamming distance is only defined for strings of equal length
    }
    let distance = 0;
    for (let i = 0; i < string_a.length; i++) {
        if (string_a[i] !== string_b[i]) {
            distance++;
        }
    }
    return distance;
$$
;





