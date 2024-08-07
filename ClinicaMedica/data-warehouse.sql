SELECT 
    DC.departamentoClinica,
    P.tipoDeProducto,
    COALESCE(G.Producto, I.Producto) AS Producto,
    I.Ingresos,
    G.Gastos,
    COALESCE(G.Quarter, I.Quarter) AS Quarter,
    COALESCE(G.Year, I.Year) AS Year,
    COALESCE(G.departamento_ClinicaID, I.departamento_ClinicaID) AS departamento_ClinicaID,
    COALESCE(G.TransactionID, I.TransactionID) AS TransactionID
FROM 
    [data-warehouse].[dbo].Gastos G
FULL OUTER JOIN 
    [data-warehouse].[dbo].Ingresos I ON G.TransactionID = I.TransactionID
LEFT JOIN 
    [data-warehouse].[dbo].DepartamentoClinica DC ON G.departamento_ClinicaID = DC.departamento_ClinicaID
                                               OR I.departamento_ClinicaID = DC.departamento_ClinicaID
LEFT JOIN
    [data-warehouse].[dbo].Productos P ON COALESCE(G.Producto, I.Producto) = P.Producto