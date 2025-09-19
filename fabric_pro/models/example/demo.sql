SELECT 
    p.FirstName, 
    p.LastName, 
    e.JobTitle 
FROM [Person_Person] p
INNER JOIN [HR.Employee] e 
    ON p.BusinessEntityID = e.BusinessEntityID


/*
select * from DimPersonEmployee where FirstName = 'John' AND LastName = 'Doe'

--Insert records into person_employee
INSERT INTO dbo.person_employee (FirstName, LastName, JobTitle)
VALUES
  ('John', 'Doe', 'Senior Developer'),
  ('Jane', 'Smith', 'Manager')

--Update Existing Employee to Trigger SCD2 Change
  UPDATE dbo.person_employee
SET JobTitle = 'Developer'
WHERE FirstName = 'John' AND LastName = 'Doe';

--Count Records
SELECT COUNT(*) AS total_src FROM dbo.person_employee
SELECT COUNT(*) AS total_stg FROM dbo.StgPersonEmployee
SELECT COUNT(*) AS total_dim FROM dbo.DimPersonEmployee

--Check One Employee's History 
SELECT *
FROM dbo.DimPersonEmployee
WHERE FirstName='John' AND LastName='Doe'
ORDER BY ValidFrom;

--Check Historical Records
SELECT COUNT(*) AS history_count
FROM dbo.DimPersonEmployee
WHERE IsCurrent=0;


  UPDATE dbo.person_employee
SET JobTitle = 'Developer'
WHERE FirstName = 'John' AND LastName = 'Doe'

INSERT INTO dbo.person_employee (FirstName, LastName, JobTitle)
VALUES
  ('John', 'Doe', 'Senior Developer'),
  ('Jane', 'Smith', 'Manager')*/