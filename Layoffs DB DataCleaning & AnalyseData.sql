/* MYSQL LAY_OFFS CLEANING, PROCES AND ANALYSE DATA PROJECT.
Thanks to Alex the Analyst https://github.com/AlexTheAnalyst/MySQL-YouTube-Series/blob/main/layoffs.csv
Analysis done by Lucas Eloy Fernandez, Data analysit JR. */

SELECT *
FROM layoffs;

-- 1. DUPLICATE THE DATA BEFORE MAKING ANY CHANGES, SO WE ARE GOING TO WORK IN A SAFE ENVIORMENT IF WE MAKE ANY MISTAKES.
CREATE TABLE layoffs_stagging
LIKE layoffs;

INSERT layoffs_stagging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_stagging;

-- 2. REMOVE DUPLICATES.
/* Because there is no ID values, or Row number to identificate every single unique row we have to remove the duplicates in a certain way.*/

SELECT *,
ROW_NUMBER () OVER(PARTITION BY 
company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging;

-- WE CREATE A ANOTHER TABLE WITH AN ADITIONAL ROW NAME "ROW_NUM" TO IDENTIFICATE THE DUPLICATES.
CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` integer
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_stagging2;

INSERT INTO layoffs_stagging2
SELECT *,
ROW_NUMBER () OVER(PARTITION BY 
company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging;

-- NOW WE SET APPART THE DUPLICATES AND DELETE THE DATA
SELECT *
FROM layoffs_stagging2
WHERE row_num > 1;

DELETE 
FROM layoffs_stagging2
WHERE row_num > 1;

-- 3. STANDARIZE DATA.
SELECT company, TRIM(company)
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_stagging2
ORDER BY 1;

SELECT *
FROM layoffs_stagging2
WHERE industry LIKE "crypto%";

UPDATE layoffs_stagging2
SET industry = "Crypto"
WHERE industry LIKE "crypto%";

SELECT DISTINCT(location) 
FROM layoffs_stagging2
ORDER BY location;

SELECT *
FROM layoffs_stagging2
WHERE location LIKE "%malm%";

UPDATE layoffs_stagging2
SET location = "Malmo"
WHERE location LIKE "%malm%";

SELECT *
FROM layoffs_stagging2
WHERE location LIKE "%sseldorf%";

UPDATE layoffs_stagging2
SET location = "Dusseldorf"
WHERE location LIKE "%sseldorf%";

SELECT DISTINCT(country) 
FROM layoffs_stagging2
ORDER BY 1;

UPDATE layoffs_stagging2
SET country = "United States"
WHERE country LIKE "%United States%";

-- CHANGE THE "DATE" VALUE FROM STRING TO DATE.
SELECT date, str_to_date(date, "%m/%d/%Y")
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET date = str_to_date(date, "%m/%d/%Y");

SELECT date
FROM layoffs_stagging2;

ALTER TABLE layoffs_stagging2
MODIFY COLUMN date DATE;

-- 4. NULL VALUES OR BLANK VALUES MUST BE REMOVE.
SELECT *
FROM layoffs_stagging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_stagging2
WHERE industry IS NULL 
OR industry = "";

UPDATE layoffs_stagging2
SET industry = NULL 
WHERE industry = "";

SELECT *
FROM layoffs_stagging2
WHERE company = "Airbnb";

SELECT t1.industry, t2.industry
FROM layoffs_stagging2 AS t1
	JOIN layoffs_stagging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = "")
AND t2.industry IS NOT NULL;

UPDATE layoffs_stagging2 AS t1
JOIN layoffs_stagging2 AS t2
	ON t1.company = t2.company
	SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_stagging2
WHERE industry IS NULL;

SELECT *
FROM layoffs_stagging2
WHERE company LIKE "%Bally%";

-- 5. REMOVE IRRELEVEANT COLUMNS
SELECT *
FROM layoffs_stagging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_stagging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_stagging2
DROP COLUMN row_num;

-- PROCESS AND ANALYZE DATA STAGE. 

SELECT * 
FROM layoffs_stagging2;

-- WE WANT TO KNOW WICH COMPANY WAS THE WORST IN TOTAL LAID OFF
SELECT company, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

-- WE WANT TO KNOW THE MAX AND MIN TOTALS LAID OFF
SELECT MAX(date), MIN(date)
FROM layoffs_stagging2;

-- WE WANT TO KNOW WICH INDUSTRY WAS THE WORST IN TOTAL LAID OFF
SELECT industry, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY industry
ORDER BY 2 DESC;

-- WE WANT TO KNOW WICH OUNTRY WAS THE WORST IN TOTAL LAID OFF
SELECT country, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY country
ORDER BY 2 DESC;

-- WE WANT TO KNOW WICH YEAR WAS THE WORST IN TOTAL LAID OFF
SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY  YEAR(date)
ORDER BY 1 DESC;

SELECT *
FROM layoffs_stagging2;

-- WE CALCULATE THE TOTAL LAID_OFFS OF EVERY MONTH
SELECT substring(date,1,7) AS "month", SUM(total_laid_off) AS laid_off
FROM layoffs_stagging2
WHERE substring(date,1,7) IS NOT NULL
GROUP BY substring(date,1,7)
ORDER BY 1 ASC;

-- NOW WE DO A ROLLING SUM UP 
WITH Rolling_Total AS (
SELECT substring(date,1,7) AS "MONTH", SUM(total_laid_off) AS total_off
FROM layoffs_stagging2
WHERE substring(date,1,7) IS NOT NULL
GROUP BY 1
ORDER BY "MONTH" ASC
)
SELECT MONTH, total_off, SUM(total_off) OVER(ORDER BY MONTH) AS rolling_total
FROM Rolling_Total;

-- WE CALCULATE THE LAID OFF´S OF EVERY COMPANY BY YEAR WITH A CTE 
SELECT company, YEAR(date), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company, YEAR(date)
ORDER BY SUM(total_laid_off) DESC;

WITH Company_year_laidoff (company, years, total_laidOff) AS(
SELECT company, YEAR(date), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company, YEAR(date)
), company_year_rank AS 
(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laidOff DESC) AS ranking
FROM Company_year_laidoff
WHERE years IS NOT NULL)
SELECT *
FROM company_year_rank
WHERE ranking <= 5;
