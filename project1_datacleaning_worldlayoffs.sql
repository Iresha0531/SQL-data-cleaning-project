SELECT *
FROM layoffs
;

CREATE TABLE dup_layoffs
LIKE layoffs
;

SELECT *
FROM dup_layoffs
;

INSERT INTO dup_layoffs
SELECT *
FROM layoffs
;

SELECT *, row_number() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
AS row_num
FROM dup_layoffs
;

WITH cte_dup_layoffs AS (
	SELECT *, row_number() OVER (
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
	AS row_num
	FROM dup_layoffs
    )
SELECT *
FROM cte_dup_layoffs
WHERE row_num > 1
;

CREATE TABLE `dup_layoffs1` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO dup_layoffs1
SELECT *, row_number() OVER (
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
	AS row_num
FROM dup_layoffs
;

SELECT *
FROM dup_layoffs1
WHERE row_num > 1;

DELETE
FROM dup_layoffs1
WHERE row_num > 1;

SELECT *
FROM dup_layoffs1;

SELECT DISTINCT company
FROM dup_layoffs1
;

SELECT company, trim(company)
from dup_layoffs1
;

UPDATE dup_layoffs1
SET company = trim(company)
;

SELECT DISTINCT industry
FROM dup_layoffs1
ORDER BY industry;

SELECT *
FROM dup_layoffs1
WHERE industry LIKE 'crypto%'
;

UPDATE dup_layoffs1
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

SELECT *
FROM dup_layoffs1;

SELECT DISTINCT country
FROM dup_layoffs1
ORDER BY 1;

SELECT DISTINCT country
FROM dup_layoffs1
WHERE country LIKE 'United States%';

SELECT country, trim(trailing '.' from country)
FROM dup_layoffs1
;

SELECT distinct country , trim(trailing '.' from country)
FROM dup_layoffs1
order by 1
;

UPDATE dup_layoffs1
SET country = trim(trailing '.' from country)
;

SELECT *
FROM dup_layoffs1
;

SELECT `date`
FROM dup_layoffs1;

select * , str_to_date(`date` , '%m/%d/%Y')
from dup_layoffs1
;

update dup_layoffs1
set `date` = str_to_date(`date` , '%m/%d/%Y')
;

SELECT *
FROM dup_layoffs1
;

ALTER TABLE dup_layoffs1
MODIFY COLUMN `date` DATE 
;

SELECT *
FROM dup_layoffs1
;

SELECT * 
FROM dup_layoffs1
WHERE industry IS NULL OR industry = ' '
;

select t1.company, t1.industry, t2.company, t2.industry
from dup_layoffs1 t1
join dup_layoffs1 t2
	on t1.company = t2.company
    and t1.country = t2.country
where t1.industry = '' or t1.industry is null 
and t2.industry is not null
;

update dup_layoffs1
set industry = NULL 
where industry = ''
;

UPDATE dup_layoffs1 t1
JOIN dup_layoffs1 t2
	ON t1.company = t2.company
    AND t1.country = t2.country
SET t1.industry = t2.industry
WHERE  t1.industry IS NULL
AND t2.industry IS NOT NULL
;

SELECT *
FROM dup_layoffs1
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

DELETE
FROM dup_layoffs1
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT *
FROM dup_layoffs1;

ALTER TABLE dup_layoffs1
DROP COLUMN row_num
;
SELECT *
FROM dup_layoffs1;


















