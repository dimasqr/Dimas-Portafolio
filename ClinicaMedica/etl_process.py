import pandas as pd
import os
import pyodbc
import math
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Classes.SQLServer import sqlServerHelper
from sqlalchemy import create_engine

DIRECTORY = r'C:\Users\dimas\OneDrive\Desktop\Portafolio\ClinicaMedica'

class ETLProcess:
    def __init__(self) -> None:
        print("Starting Process.")
        self.expenses_df = self.expenses()
        self.incomes_df = self.incomes()
        self.products_df = self.products()
        self.clinics_df = self.create_allLocation_df()
        self.normalized_tables()
        self.dbUploader("Incomes")
        self.dbUploader("Expenses")
        self.dbUploader("Products")
        self.dbUploader("Locations")
        print("Process has been completed.")
        
    def dirMaker(self, transaction_type):
        """Creates a directory based on transaction type."""
        print("Building directories")
        return os.path.join(DIRECTORY, transaction_type)

    def readFiles(self, transaction_type, file_extensions=('.xlsx', '.xls')):
        """Load files with specified extensions."""
        self.data_frames = []
        dir_path = self.dirMaker(transaction_type)
        for file_name in os.listdir(dir_path):
            if file_name.endswith(file_extensions):
                file_path = os.path.join(dir_path, file_name)
                print(f"Reading file for {transaction_type}")
                try:
                    df = pd.read_excel(file_path)
                    self.data_frames.append(df)
                except Exception as e:
                    print(f"Error reading the file: {file_name}, {e}")

    def consolidate_data(self, transaction_type, column_money):
        """Consolidate loaded data into one."""
        if not self.data_frames:
            print("There is no data to consolidate.")
            return None

        if transaction_type == "Products":
            combined_df = pd.concat(self.data_frames, ignore_index=True)
            combined_df = combined_df.rename(columns={
                'Product Type' : 'product_type',
                'Product' : 'product'
            })
            return combined_df
        
        combined_df = pd.concat(self.data_frames, ignore_index=True)
        combined_df = combined_df.rename(columns={'Hospital Location':'location'})
        combined_df = combined_df.groupby(['Period', 'Product', 'location'])[column_money].sum().reset_index()
        combined_df[['Quarter', 'Year']] = combined_df['Period'].str.split(' ', expand=True)
        combined_df = combined_df.drop(columns=['Period'])
        print(f"Data frames(s) for {transaction_type} have been created.")
        return combined_df

    def expenses(self):
        transaction_type = "Expenses"
        self.readFiles(transaction_type)
        return self.consolidate_data(transaction_type, "Expenses")
    
    def incomes(self):
        transaction_type = "Incomes"
        self.readFiles(transaction_type)
        return self.consolidate_data(transaction_type, "Incomes")
    
    def products(self):
        transaction_type = "Products"
        self.readFiles(transaction_type)
        return self.consolidate_data(transaction_type, None)

    def create_allLocation_df(self):
        """Create a DataFrame with unique locations from expenses and incomes."""
        all_locations_df = pd.concat([self.expenses_df['location'], self.incomes_df['location']]).drop_duplicates().reset_index(drop=True)
        all_locations_df = all_locations_df.to_frame(name='location')
        all_locations_df['locationID'] = all_locations_df.index + 1
        return all_locations_df
    
    def normalize_table(self, df):
        df = pd.merge(df, self.clinics_df, on='location', how='left')
        df = df.drop(columns=['location'])
        df['transactionID'] = df['locationID'].astype(str) + df['Quarter'].astype(str) + df['Year'].astype(str) + df['Product'].astype(str)
        return df

    def normalized_tables(self):
        self.n_expenses_df = self.normalize_table(self.expenses_df)
        self.n_incomes_df = self.normalize_table(self.incomes_df)

    
    def dbUploader(self, table_name):
        """Upload data to SQL Server database."""
        table_mapping = {
            "Incomes": (self.n_incomes_df, sqlServerHelper.getIncomesSchema, sqlServerHelper.getIncomesValues),
            "Expenses": (self.n_expenses_df, sqlServerHelper.getExpensesSchema, sqlServerHelper.getExpensesValues),
            "Products": (self.products_df, sqlServerHelper.getProductsSchema, sqlServerHelper.getProductsValues),
            "Locations": (self.clinics_df, sqlServerHelper.getLocationsSchema, sqlServerHelper.getLocationsValues)
        }
        
        if table_name not in table_mapping:
            raise ValueError(f"Table name {table_name} is not recognized.")
        
        df_to_upload, schema_func, values_func = table_mapping[table_name]

        conn = pyodbc.connect(sqlServerHelper.getConnectionName())
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        schema = schema_func()
        for index, row in df_to_upload.iterrows():
            cursor.execute(schema, *values_func(row))
        
        print(f"{table_name} has been updated sucessfully.")
        
        conn.commit()
        conn.close()

ETLProcess()