class sqlServerHelper:
    
    @staticmethod
    def getConnectionName():
        return 'DRIVER={SQL Server};SERVER=DIMASQUINTANA;DATABASE=data-warehouse;UID=sa;PWD=1010'
    
    @staticmethod
    def getIncomesSchema():
        return (
            "INSERT INTO Incomes (Product, Incomes, Quarter, Year, locationID, transactionID) VALUES (?, ?, ?, ?, ?, ?)"
        )
    
    @staticmethod
    def getIncomesValues(row):
        return (
            row['Product'], 
            row['Incomes'], 
            row['Quarter'], 
            row['Year'], 
            row['locationID'], 
            row['transactionID']
        )
    
    @staticmethod
    def getExpensesSchema():
        return (
            "INSERT INTO Expenses (Product, Expenses, Quarter, Year, locationID, transactionID) VALUES (?, ?, ?, ?, ?, ?)"
        )
    
    @staticmethod
    def getExpensesValues(row):
        return (
            row['Product'], 
            row['Expenses'], 
            row['Quarter'], 
            row['Year'], 
            row['locationID'], 
            row['transactionID']
        )

    @staticmethod
    def getProductsSchema():
        return (
            "INSERT INTO Products (Product, product_type) VALUES (?, ?)"
        )
    
    @staticmethod
    def getProductsValues(row):
        return (
            row['product'], 
            row['product_type']
        )

    @staticmethod
    def getLocationsSchema():
        return (
            "INSERT INTO Locations (location, locationID) VALUES (?, ?)"
        )
    
    @staticmethod
    def getLocationsValues(row):
        return (
            row['location'], 
            row['locationID']
        )