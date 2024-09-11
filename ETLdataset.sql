-- Create the database if it doesn't already exist
CREATE DATABASE IF NOT EXISTS ETLDemo;

-- Switch to the new database
USE ETLDemo;

-- Drop the table if it exists
DROP TABLE IF EXISTS Expenses;

-- Create the table with the specified schema
CREATE TABLE Expenses (
    `date` DATETIME,
    `rate` DECIMAL(6, 4),
    `USD` DECIMAL(10, 2),
    `CAD` DECIMAL(10, 2)
);
