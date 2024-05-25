-- DATA CLEANING
CREATE SCHEMA WORLD_LAYOFF;

-- rIght clIck schema and clck tabledata Import wIzard
-- browse, clIck desIred table, and mport
use world_layoffs;
select * from layoffs;


-- DATA CLEANING PROCESS 
-- 1. REMOVE DUPLICATEda
-- 2. STANDARDIZE DATA
-- 3. NULL VALUE OR BLANK VALUES 
-- 4. REMOVE ANY COLUMNS


-- DUPLICATE THE DATASET
CREATE TABLE layoff_staging
like layoffs;

select * from layoff_staging;

insert layoff_staging
select * from layoffs;

-- identifying duplicates
select * ,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off, 
percentage_laid_off,"date", stage,country,funds_raised_millions) as row_num
from layoff_staging;

with duplicate_cte AS 
(
select * ,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, date, stage,country, funds_raised_millions) as row_num
from layoff_staging
)
select *
from duplicate_cte
where row_num > 1;

-- check 
select * from layoff_staging
where company = 'Better.com';

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num INT
);

select * from layoff_staging2;

insert into layoff_staging2
select * ,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, date, stage,country, funds_raised_millions) as row_num
from layoff_staging;

select * from layoff_staging2
where row_num > 1;
-- before delete, always use select statement to confirm
-- delete duplicate

delete from layoff_staging2
where row_num > 1;
-- check
select * from layoff_staging2;




-- 2. standardizing data

select DISTINCT(company)
from layoff_staging2;

-- trim =take whte space off

select DISTINCT(trim(company))
from layoff_staging2;

select company, trim(company)
from layoff_staging2;
-- updating whitespace error
update layoff_staging2
set company = trim(company);
-- checking industry

select DISTINCT(industry)
from layoff_staging2
ORDER BY 1;

select DISTINCT(industry)
from layoff_staging2
ORDER BY 1;

select *
from layoff_staging2
where industry = 'crypto%' ;

select *
from layoff_staging2
where industry LIKE 'crypto%'  ;
-- updating column with name issue
update layoff_staging2
set industry = 'Crypto'
where industry LIKE 'crypto%' ;

-- checking country
select DISTINCT(COUNTRY)
from layoff_staging2
ORDER BY 1;

-- trim(TRAILING '.'  FROM COUNTRY) REMOVES . FROM END
select DISTINCT(COUNTRY), trim(TRAILING '.'  FROM COUNTRY)
from layoff_staging2
ORDER BY 1;

UPDATE layoff_staging2
SET COUNTRY = trim(TRAILING '.'  FROM COUNTRY)
WHERE COUNTRY LIKE 'UNITED STATES%' ;

-- CHANGE DATE FORMAT
SELECT date
FROM layoff_staging2;

-- converts strings to date
SELECT date,
str_to_date(date, '%m/%d/%Y')
FROM layoff_staging2;

UPDATE layoff_staging2
set date = str_to_date(date, '%m/%d/%Y');

ALTER TABLE layoff_staging2
MODIFY COLUMN date DATE;

-- CHECKNG for null

SELECT * 
FROM layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- null industry
SELECT DISTINCT industry
FROM layoff_staging2;

SELECT *
FROM layoff_staging2
WHERE industry is null
OR industry= '' ;

SELECT *
FROM layoff_staging2
WHERE company = 'Airbnb' ;

-- change blanks to null
UPDATE layoff_staging2
SET industry = NULL
WHERE industry = '' ; 

-- check
 SELECT t1.industry, t2.industry
 FROM layoff_staging2 t1
 JOIN layoff_staging2 t2
 ON t1.company = t2.company
 WHERE(t1.industry is null or t1.industry= '')
 AND t2.industry is not null;
 
 -- UPDATE nulls
 UPDATE layoff_staging2 t1
 JOIN layoff_staging2 t2
 ON t1.company = t2.company
 SET t1.industry = t2.industry
 WHERE t1.industry is null 
 AND t2.industry is not null;
  
  -- lookng at useful columns
  -- DELETE columns WITHout total_laid_off and percentage_laid_off
  SELECT * 
FROM layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

DELETE
FROM layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

 -- DELETE column row num
 ALTER TABLE layoff_staging2
 DROP COLUMN row_num;
 -- check
   SELECT * 
FROM layoff_staging2;
-- We couldnt fill in nulls in total_laid_off and percentage_laid_off because we dint have total workers for the 
-- organisations in our data
-- THE END
-- THANK YOU 
 