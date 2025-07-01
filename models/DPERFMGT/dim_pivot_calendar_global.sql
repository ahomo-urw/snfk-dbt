/* V1 non dynamique
CREATE OR REPLACE TABLE EXPOSE_DEV.DPERFMGT.DIM_CALENDAR(
    ID NUMBER,
    CAL_PIVOT VARCHAR(13),
    CAL_ID VARCHAR(6),
    CAL_TYPE VARCHAR(10)
) COMMENT = 'Creation table de dimension calendaire EGELFI 2025' AS 
-- Génération des mois
WITH CAL_ID AS (
    SELECT 
        TO_CHAR(DATEADD(MONTH, SEQ4(), '2005-01-01'), 'YYYYMM') AS CAL_ID
    FROM TABLE(GENERATOR(ROWCOUNT => 432)) -- 35 ans * 12 mois = 432 => 2005 à 2040
)
-- Création des combinaisons de mois début-fin
, CAL_PIVOT AS (
    SELECT 
        m1.CAL_ID || '-' || m2.CAL_ID AS CAL_PIVOT
    FROM CAL_ID m1
    INNER JOIN CAL_ID m2
        ON m2.CAL_ID >= m1.CAL_ID
)
-- Création du type
, CAL_TYPES AS (
    SELECT 'DEB' AS CAL_TYPE
    UNION ALL
    SELECT 'En cours'
    UNION ALL
    SELECT 'FIN'
)
-- Liaison CAL_PIVOT, CAL_ID
, CAL_PIVOT_ID AS (
    SELECT 
        p.CAL_PIVOT,
        c.CAL_ID
    FROM CAL_PIVOT p
    INNER JOIN CAL_ID c ON c.CAL_ID BETWEEN SUBSTR(p.CAL_PIVOT, 1, 6) AND SUBSTR(p.CAL_PIVOT, 8, 6)
)
-- Liaison CAL_PIVOT, CAL_ID, CAL_TYPE et ajout d'un ID unique
, CAL_PIVOT_ID_TYPE AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY c.CAL_PIVOT, c.CAL_ID, t.CAL_TYPE) AS ID,
        c.CAL_PIVOT,
        c.CAL_ID,
        t.CAL_TYPE
    FROM CAL_PIVOT_ID c
    CROSS JOIN CAL_TYPES t
    ORDER BY c.CAL_PIVOT, c.CAL_ID
)
SELECT * FROM CAL_PIVOT_ID_TYPE;
*/
-- drop table EXPOSE_DEV.DPERFMGT.DIM_CALENDAR;
-- select * from EXPOSE_DEV.DPERFMGT.DIM_CALENDAR;

-- V2 Dynamique
CREATE OR REPLACE TABLE EXPOSE_DEV.DPERFMGT.DIM_PIVOT_CALENDAR_GLOBAL(
    ID NUMBER,
    CAL_PIVOT VARCHAR(13),
    CAL_ID VARCHAR(6),
    CAL_TYPE VARCHAR(10)
) COMMENT = 'Creation table de dimension calendaire EGELFI 2025' AS 
-- Récupération des bornes MIN et MAX
--select distinct YM_START_END from expose_dev.dperfmgt.t_fact_performance_global;
WITH DATE_BOUNDS AS (
    SELECT 
        TO_DATE(MIN(LEFT(YM_START_END, 4)) || '-01-01') AS MIN_DATE,
        TO_DATE('2040-12-31')  AS MAX_DATE  --TO_DATE(MAX(substring(YM_START_END, 7,11)) || '-12-31') AS MAX_DATE -- Forcer fin d'année
    FROM EXPOSE_DEV.DPERFMGT.T_FACT_PERFORMANCE_GLOBAL
)
-- Génération d’un grand nombre de mois (ex. 600)
, CAL_ID_RAW AS (
    SELECT 
        DATEADD(MONTH, SEQ4(), '2000-01-01') AS CAL_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 600))
)
-- Filtrage selon les bornes dynamiques
, CAL_ID AS (
    SELECT 
        TO_CHAR(CAL_DATE, 'YYYYMM') AS CAL_ID
    FROM CAL_ID_RAW, DATE_BOUNDS
    WHERE CAL_DATE BETWEEN DATE_BOUNDS.MIN_DATE AND DATE_BOUNDS.MAX_DATE
)
-- Création des combinaisons de mois début-fin
, CAL_PIVOT AS (
    SELECT 
        m1.CAL_ID || '-' || m2.CAL_ID AS CAL_PIVOT
    FROM CAL_ID m1
    INNER JOIN CAL_ID m2
        ON m2.CAL_ID >= m1.CAL_ID
)
-- Liaison CAL_PIVOT, CAL_ID
, CAL_PIVOT_ID AS (
    SELECT 
        p.CAL_PIVOT,
        c.CAL_ID
    FROM CAL_PIVOT p
    INNER JOIN CAL_ID c ON c.CAL_ID BETWEEN SUBSTR(p.CAL_PIVOT, 1, 6) AND SUBSTR(p.CAL_PIVOT, 8, 6)
)
-- Liaison CAL_PIVOT, CAL_ID, CAL_TYPE et ajout d'un ID unique
, CAL_PIVOT_ID_TYPE AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY c.CAL_PIVOT, c.CAL_ID) AS ID,
        c.CAL_PIVOT,
        c.CAL_ID,
        case 
            when CAL_ID = left (c.CAL_PIVOT,6) then 'START'
            when CAL_ID = right (c.CAL_PIVOT,6) then 'END'
            else 'PENDING'
        end as CAL_TYPE
    FROM CAL_PIVOT_ID c
--    CROSS JOIN CAL_TYPES t
    ORDER BY c.CAL_PIVOT, c.CAL_ID
)
SELECT * FROM CAL_PIVOT_ID_TYPE order by CAL_ID,CAL_PIVOT desc;
