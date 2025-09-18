SELECT 
    p.FirstName, 
    p.LastName, 
    e.JobTitle 
FROM [Person_Person] p
INNER JOIN [HR.Employee] e 
    ON p.BusinessEntityID = e.BusinessEntityID



