import pandas as pd
import os
import pyodbc
from sqlalchemy import create_engine

print("Iniciando...")

##################################################################### Importar gastos #####################################################################

gastos_directorio = r'C:\Users\dimas\OneDrive\Documents\Prueba\gastos'

gastos_df = []

#Itera sobre cada archivo en el directorio
for archivo_gastos in os.listdir(gastos_directorio):
    if archivo_gastos.endswith('.xlsx') or archivo_gastos.endswith('.xls'): #Revisa si la carpeta tiene archivos xlsx
        ruta_gastos = os.path.join(gastos_directorio, archivo_gastos) #Construye la direccion de cada archivo
        try:
            gastos_doc = pd.read_excel(ruta_gastos) #Lee los Excel en un solo data frame
            gastos_df.append(gastos_doc) #adjunta el data frame a la lista vacia
        except Exception as e:
            print(f"Error al leer el archivo de gastos: {archivo_gastos}: {e}")

#Une los archivos en una sola df
gastos_combinados = pd.concat(gastos_df, ignore_index=True)
gastos_combinados = gastos_combinados.rename(columns={'Departamento - Clinica':'departamentoClinica'})
gastos_combinados = gastos_combinados.groupby(['Periodo', 'Producto', 'departamentoClinica'])['Gastos'].sum().reset_index()
gastos_combinados[['Quarter', 'Year']] = gastos_combinados['Periodo'].str.split(' ', expand=True)
gastos_combinados = gastos_combinados.drop(columns=['Periodo'])

##################################################################### Importar ingresos #####################################################################

ingresos_directorio = r'C:\Users\dimas\OneDrive\Documents\Prueba\ingresos'

ingresos_df = []

#Itera sobre cada archivo en el directorio
for archivo_ingresos in os.listdir(ingresos_directorio):
    if archivo_ingresos.endswith('.xlsx') or archivo_ingresos.endswith('.xls'): 
        ruta_ingresos = os.path.join(ingresos_directorio, archivo_ingresos)
        try:
            ingresos_doc = pd.read_excel(ruta_ingresos)
            ingresos_df.append(ingresos_doc)
        except Exception as e:
            print(f"Error al leer el archivo de ingresos: {archivo_ingresos}: {e}")

#Une los archivos en una sola df
ingresos_combinados = pd.concat(ingresos_df, ignore_index=True)
ingresos_combinados = ingresos_combinados.rename(columns={'Departamento - Clinica':'departamentoClinica'})
ingresos_combinados = ingresos_combinados.groupby(['Periodo', 'Producto', 'departamentoClinica'])['Ingresos'].sum().reset_index()
ingresos_combinados[['Quarter', 'Year']] = ingresos_combinados['Periodo'].str.split(' ', expand=True)
ingresos_combinados = ingresos_combinados.drop(columns=['Periodo'])

#################################################################### Departamento - clinica #####################################################################

departamento_clinica = pd.concat([gastos_combinados['departamentoClinica'],ingresos_combinados['departamentoClinica']])
departamento_clinica = departamento_clinica.drop_duplicates().to_frame(name='departamentoClinica')
departamento_clinica = departamento_clinica.reset_index()
departamento_clinica['departamento_clinicaID'] = departamento_clinica.index+1
departamento_clinica = departamento_clinica.drop(columns=['index'])

#################################################################### Tablas normalizadas ########################################################################

gastos_combinados = pd.merge(gastos_combinados, departamento_clinica, on='departamentoClinica', how='left')
gastos_combinados = gastos_combinados.drop(columns=['departamentoClinica'])
gastos_combinados['TransactionID'] = gastos_combinados['departamento_clinicaID'].astype(str) + gastos_combinados['Quarter'].astype(str) + gastos_combinados['Year'].astype(str) + gastos_combinados['Producto'].astype(str)

ingresos_combinados = pd.merge(ingresos_combinados, departamento_clinica, on='departamentoClinica', how='left')
ingresos_combinados = ingresos_combinados.drop(columns=['departamentoClinica'])
ingresos_combinados['TransactionID'] = ingresos_combinados['departamento_clinicaID'].astype(str) + ingresos_combinados['Quarter'].astype(str) + ingresos_combinados['Year'].astype(str) + ingresos_combinados['Producto'].astype(str)

##################################################################### Importar productos #####################################################################

productos_directorio = r'C:\Users\dimas\OneDrive\Documents\Prueba\productos'

productos_df = []

#Itera sobre cada archivo en el directorio
for archivo_productos in os.listdir(productos_directorio):
    if archivo_productos.endswith('.xlsx') or archivo_productos.endswith('.xls'): 
        ruta_productos = os.path.join(productos_directorio, archivo_productos)
        try:
            productos_doc = pd.read_excel(ruta_productos)
            productos_df.append(productos_doc)
        except Exception as e:
            print(f"Error al leer el archivo de ingresos: {archivo_ingresos}: {e}")

#Une los archivos en una sola df
productos_combinados = pd.concat(productos_df, ignore_index=True)

#################################################################### Exportar "Productos" a SQL Server #####################################################################

print("Iniciando la exportación de 'Productos' a SQL Server.")

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=DIMASQUINTANA;DATABASE=data-warehouse;UID=sa;PWD=1010')

# Definir un cursor
cursor = conn.cursor()
cursor.execute("TRUNCATE TABLE Ingresos")

# Iterar sobre cada fila del DataFrame e insertarla en la tabla Productos
for index, row in ingresos_combinados.iterrows():
    cursor.execute("INSERT INTO Ingresos (Producto, Ingresos, Quarter, Year, departamento_clinicaID, TransactionID) VALUES (?, ?, ?, ?, ?, ?)",
                   row['Producto'], row['Ingresos'], row['Quarter'], row['Year'], row['departamento_clinicaID'], row['TransactionID'])

# Confirmar la transacción
conn.commit()
print("Exportación de 'Productos' completada.")

#################################################################### Exportar "Departamento - Clinica" a SQL Server #####################################################################

print("Iniciando la exportación de 'Departamento - Clinica' a SQL Server.")

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=DIMASQUINTANA;DATABASE=data-warehouse;UID=sa;PWD=1010')

# Definir un cursor
cursor = conn.cursor()
cursor.execute("TRUNCATE TABLE DepartamentoClinica")

# Iterar sobre cada fila del DataFrame e insertarla en la tabla Productos
for index, row in departamento_clinica.iterrows():
    cursor.execute("INSERT INTO DepartamentoClinica (departamentoClinica, departamento_clinicaID) VALUES (?, ?)",
                   row['departamentoClinica'], row['departamento_clinicaID'])

# Confirmar la transacción
conn.commit()
print("Exportación de 'Departamento - Clinica' completada.")

#################################################################### Exportar "Gastos" a SQL Server #####################################################################

print("Iniciando la exportación de 'Gastos' a SQL Server.")

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=DIMASQUINTANA;DATABASE=data-warehouse;UID=sa;PWD=1010')

# Definir un cursor
cursor = conn.cursor()
cursor.execute("TRUNCATE TABLE Gastos")

# Iterar sobre cada fila del DataFrame e insertarla en la tabla Productos
for index, row in gastos_combinados.iterrows():
    cursor.execute("INSERT INTO Gastos (Producto, Gastos, Quarter, Year, departamento_clinicaID, TransactionID) VALUES (?, ?, ?, ?, ?, ?)",
                   row['Producto'], row['Gastos'], row['Quarter'], row['Year'], row['departamento_clinicaID'], row['TransactionID'])

# Confirmar la transacción
conn.commit()
print("Exportación de 'Gastos.")

#################################################################### Exportar "Ingresos" a SQL Server #####################################################################

print("Iniciando la exportación de 'Ingresos' a SQL Server.")

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=DIMASQUINTANA;DATABASE=data-warehouse;UID=sa;PWD=1010')

# Definir un cursor
cursor = conn.cursor()
cursor.execute("TRUNCATE TABLE Ingresos")

# Iterar sobre cada fila del DataFrame e insertarla en la tabla Productos
for index, row in ingresos_combinados.iterrows():
    cursor.execute("INSERT INTO Ingresos (Producto, Ingresos, Quarter, Year, departamento_clinicaID, TransactionID) VALUES (?, ?, ?, ?, ?, ?)",
                   row['Producto'], row['Ingresos'], row['Quarter'], row['Year'], row['departamento_clinicaID'], row['TransactionID'])

# Confirmar la transacción
conn.commit()
conn.close()
print("Exportación de 'Ingresos'.")
print("Proceso completado.")
