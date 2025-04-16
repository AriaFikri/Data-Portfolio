-- Initial data viewing
SELECT * 
FROM layoffs;

-- Search for missing values in crucial columns (total_laid_off and funds_raised_millions)
SELECT company, location, total_laid_off, funds_raised_millions
FROM layoffs
WHERE total_laid_off IS NULL OR funds_raised_millions IS NULL;

-- Updating data where total laid off is null to 0
UPDATE layoffs
SET total_laid_off = 0
WHERE total_laid_off IS NULL;

-- Assessing duplicate data
WITH cte_layoff AS
(
SELECT *, ROW_NUMBER() OVER (PARTITION BY company, location, total_laid_off, percentage_laid_off, `date`, funds_raised_millions) AS row_num
FROM layoffs
)
SELECT *
FROM cte_layoff
WHERE row_num > 1;

-- Creating new table for data processing
CREATE TABLE `layoffs_staging` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` double DEFAULT NULL,
  `date` datetime DEFAULT NULL,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insert all data (plus the new row_num column to identify duplicates) into the new layoffs_staging table
INSERT INTO layoffs_staging
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY company, location, total_laid_off, percentage_laid_off, `date`, funds_raised_millions) AS row_num
FROM layoffs;

-- Deleting and checking duplicate data
DELETE
FROM layoffs_staging
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE row_num > 1;

SELECT *
FROM layoffs_staging;

-- Delete trailing empty space
UPDATE layoffs_staging
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging
ORDER BY industry;

-- Standardizing data
UPDATE layoffs_staging
SET industry = 'Crypto Currency'
WHERE industry REGEXP 'Crypto';

SELECT DISTINCT(country)
FROM layoffs_staging
ORDER BY 1;

UPDATE layoffs_staging
SET country = TRIM(TRAILING "." FROM country)
WHERE country LIKE "United States%";

SELECT `date`, CAST(`date` AS date)
FROM layoffs_staging;

SELECT *
FROM layoffs_staging;

ALTER TABLE layoffs_staging
MODIFY `date` DATE;

SELECT company, industry
FROM layoffs_staging
WHERE company LIKE "Bally%";

SELECT DISTINCT(industry)
FROM layoffs_staging
WHERE industry = "" OR industry IS NULL
ORDER BY 1;

-- Update industry to null if empty
UPDATE layoffs_staging
SET industry = NULL
WHERE industry = "";

-- Updating null industry from another row where company name is the same
SELECT st1.company, st1.industry, st2.industry
FROM layoffs_staging AS st1
JOIN layoffs_staging AS st2
	ON st1.company = st2.company
WHERE (st2.industry IS NOT NULL) AND st1.industry IS NULL;

UPDATE layoffs_staging st1
JOIN layoffs_staging AS st2
	ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE (st2.industry IS NOT NULL) AND st1.industry IS NULL;

ALTER TABLE layoffs_staging
DROP COLUMN row_num;

SELECT MAX(total_laid_off) as max_laid_off, MIN(total_laid_off) as min_laid_off, industry
FROM layoffs_staging
GROUP BY max_laid_off;

-- Query industry and its total laid off
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;

-- Query company stage and its total laid off
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;

-- Extract month from date column
SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off)
FROM layoffs_staging
WHERE total_laid_off IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

-- Query a running sum by month
WITH cte_running AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off) AS sum_off
FROM layoffs_staging
WHERE total_laid_off IS NOT NULL AND SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT *, SUM(sum_off) OVER(ORDER BY `Month`) AS running_sum
FROM cte_running;

-- Query company total laid off per year
SELECT company, YEAR(`date`) AS `Year`, sum(total_laid_off) AS sum_off
FROM layoffs_staging
GROUP BY company, `Year`
ORDER BY sum_off DESC;

-- Ranking company total laid off
WITH rank_cte (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`) AS `Year`, sum(total_laid_off) AS sum_off
FROM layoffs_staging
WHERE total_laid_off IS NOT NULL
GROUP BY company, `Year`
ORDER BY sum_off DESC
)
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS RANKING
FROM rank_cte
