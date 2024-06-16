

--DATA CLEANING THE LAYOFFS DATASET

  SELECT *
  FROM layoffs;

--Step 1: Remove Duplicates 
--Step 2: Standardize the Data 
--Step 3: Null Values or Blank values 
--Step 4: Remove Any Columns 

--The initial SELECT * INTO layoffs_staging FROM layoffs WHERE 1 = 0; creates a new table layoffs_staging that matches 
--the structure of layoffs but is initially empty.
--The subsequent INSERT INTO layoffs_staging SELECT * FROM layoffs; inserts all data from layoffs into layoffs_staging, 
--effectively populating the newly created table with the same data as layoffs.
SELECT *
INTO layoffs_staging
FROM layoffs
WHERE 1 = 0;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

--REMOVE THE DUPLICATES 
--The rows with the duplicates 
SELECT *,
       ROW_NUMBER() 
	   OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, [date], stage,country, funds_raised_millions
	   ORDER BY (SELECT NULL)) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS 
(SELECT *,
       ROW_NUMBER() 
	   OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, [date], stage,country, funds_raised_millions 
	   ORDER BY (SELECT NULL)) AS row_num
FROM layoffs_staging 
)

SELECT*
FROM duplicate_cte
WHERE row_num > 1;


SELECT*
FROM layoffs_staging
WHERE company ='Casper';

--Create an extra empty table labeled layoffs_staging2

CREATE TABLE [dbo].[layoffs_staging2](
	[company] [nvarchar](255) NULL,
	[location] [nvarchar](255) NULL,
	[industry] [nvarchar](255) NULL,
	[total_laid_off] [float] NULL,
	[percentage_laid_off] [nvarchar](255) NULL,
	[date] [datetime] NULL,
	[stage] [nvarchar](255) NULL,
	[country] [nvarchar](255) NULL,
	[funds_raised_millions] [float] NULL,
	[row_num] [int] NULL 
) ON [PRIMARY]
GO

SELECT *
FROM layoffs_staging2;
--Inserted data into the table
INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() 
	   OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, [date], stage,country, funds_raised_millions 
	   ORDER BY (SELECT NULL)) AS row_num
FROM layoffs_staging;

--Delete the duplicates from the layoffs_staging table
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;


SELECT* 
FROM layoffs_staging2;


--STANDARDIZING DATA 
--Means finding issues with the data and dealing with them 

SELECT company, TRIM(company)
FROM layoffs_staging2;

--UPDATE THE TABLE 
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT*
FROM layoffs_staging2
WHERE industry LIKE 'Crypto';


SELECT*
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States.%'
ORDER BY 1;
--Remove the dots at the end of the country
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT *
FROM layoffs_staging2;
SELECT 
    [date],
    CONVERT(DATE, [date], 101) AS converted_date,
    DATEPART(MONTH, CONVERT(DATE, [date], 101)) AS month,
    DATEPART(DAY, CONVERT(DATE, [date], 101)) AS day,
    DATEPART(YEAR, CONVERT(DATE, [date], 101)) AS year
FROM 
    layoffs_staging2;

--Change the datatype of the date column
ALTER TABLE layoffs_staging2
ALTER COLUMN [date] DATE;

--Check the layoffs_staging2 table 
SELECT *
FROM layoffs_staging2;

--CHECKING FOR THE NULL VALUES 
--Checking for the null values in the total-laid off table 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL ;


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT*
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
     ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL

--Updating the t1 
UPDATE t1
SET t1.industry = t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
WHERE (t1.industry IS NULL )
AND t2.industry IS NOT NULL;


--Delete the rows with the null values 
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

--Drop the rownum column 
ALTER TABLE  layoffs_staging2
DROP COLUMN row_num;














































