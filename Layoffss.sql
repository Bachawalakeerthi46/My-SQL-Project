-- DATA CLEANING


SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null values or Blank Values
-- 4. Remove any columns or rows

-- Here we are duplicating the layoffs table into the layoffs_staging to perform further tasks

-- We are creating a layoffs_staging TABLE same as layoffs
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Selecting all the data from layoffs_staging. initially it only creates the structure
SELECT * 
FROM layoffs_staging;

-- Inserting everything from layoffs table to layoffs_staging
INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- 1. Remove Duplicates

-- Removing duplicates and assigning the row numbers to each and every row
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


-- Creating a table layoffs_staging3, with referring to layoffs_staging and adding data type to the row_num
CREATE TABLE `layoffs_staging3` (
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


SELECT *
FROM layoffs_staging3;

-- Inserting all the data from layoffs_staging to layoffs_staging3
INSERT layoffs_staging3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY Company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;

-- Deleting the row_numbers which are greater than '1' 
DELETE 
FROM layoffs_staging3
WHERE row_num > 1;


SELECT *
FROM layoffs_staging3;


-- 2. Standardize the Data // finding issues in the data and fixing it


-- TRIM IS USED TO REMOVE THE WHITE SPACES

-- Trimming any spaces or blanks from 'Comapny' column 
SELECT company, TRIM(Company)
FROM layoffs_staging3;

-- Updating modified/Trimmed column to the Company column
UPDATE layoffs_staging3
SET company = TRIM(company);

-- Selecting Unique industries in a sorting method(Order by 1)
SELECT DISTINCT industry
FROM layoffs_staging3
ORDER BY 1;

-- Selecting industry names starting with crypto
SELECT *
FROM layoffs_staging3
WHERE industry LIKE 'crypto%';

-- Updating/Setting the all of them as 'Crypto'
UPDATE layoffs_staging3
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';


SELECT *
FROM layoffs_staging3;

-- Selecting the distinct/unique location to check if there are any changes needed
SELECT DISTINCT location
FROM layoffs_staging3
ORDER BY 1;

-- Selecting the distinct/unique Country to check if there are any changes needed
SELECT DISTINCT country
FROM layoffs_staging3
ORDER BY 1;

-- Selecting the Country with name UnitedStates in a sorting way(order by)
SELECT *
FROM layoffs_staging3
WHERE country LIKE 'United States%'
ORDER BY 1;

-- Removing any spaces or dots from the Country column
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging3
ORDER BY 1;

-- Setting/Updating the Country column with the trimmed data
UPDATE layoffs_staging3
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States';


-- changing date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging3;


UPDATE layoffs_staging3
SET `date` = str_to_date(`date`, '%m/%d/%Y');


SELECT `date`
FROM layoffs_staging3;


-- CHANGING THE DATA TYPE
ALTER TABLE layoffs_staging3
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layoffs_staging3;


-- 3. Null values or Blank Values

-- Selecting the columns where Total_laid_off and Percentage_laid_off columns are Null
SELECT * 
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- industry column has null and blank values
SELECT DISTINCT industry
FROM layoffs_staging3;

-- Selecting the data where the industry column was null or blank
SELECT *
FROM layoffs_staging3
WHERE industry IS NULL
OR industry = '';

-- Firstly updating all the blanks to Null 
UPDATE layoffs_staging3
SET industry = NULL 
WHERE industry = '';

-- Selecting all the data where Company is 'Airbnb'
SELECT *
FROM layoffs_staging3
WHERE company = 'Airbnb';

-- here we see that the industry is not assigned to one of the Airbnb company
-- we tried to assign the same industry to both Airbnb companies

-- Here we are dividing the same data to table 1 and table 2
-- where table 1 shows the data with null and blank values and table 2 with not null values

SELECT *
FROM layoffs_staging3 t1
JOIN layoffs_staging3 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Updating all the Null or blank values with the help of table 2
UPDATE layoffs_staging3 t1
JOIN layoffs_staging3 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT * FROM 
layoffs_staging3;


-- remove all the rows and columns that has null. suppose total laid off column and percentage laid off

SELECT * 
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


DELETE
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- 4 Removed columns or rows

-- Deleting the column row_num
ALTER 
TABLE layoffs_staging3
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging3;







