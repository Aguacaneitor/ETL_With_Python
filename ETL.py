
#from pprint import pprint
import csv
import shutil
import os
from datetime import date
import pandas as pd
#import numpy as nd
#import matplotlib.pyplot as plt
import codecs
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def csvToSheet(crudo, origen, destino, sheet, hoja="Defecto_unica", delimitador=","):

    try:
        
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("creedsGI.json", scope)
        client = gspread.authorize(creds)
        
        if hoja == "Defecto_unica":
            sheet2 = client.open_by_url(sheet).sheet1
        else:
            sheet2 = client.open_by_url(sheet).worksheet(hoja)

        contenidoCSV = []

    except:
        print("error al conectar con el sheet.")
    try:
        lista = os.listdir(origen)
        for archivo in lista:
            if archivo == crudo:
                with codecs.open(origen + "/" + archivo, 'r', encoding="utf-8") as csv_file:
                    csv_Reader = csv.reader(csv_file, delimiter=delimitador)
                    # next(csv_Reader)
                    for linea in csv_Reader:
                        contenidoCSV.append(linea)

                shutil.move(origen + "/" + archivo, destino + "/" +
                            date.today().strftime("%d-%m-%Y") + archivo)
                sheet2.clear()
                sheet2.update(contenidoCSV)
    except:
        print("No procesado el archivo")


def excelToCsv(crudo, origen, destino, nombreSheet=0):
    crudoSinFormato = crudo.rsplit(".", 1)[0]
    try:
        lista = os.listdir(origen)
        for archivo in lista:
            if archivo == crudo:
                dato = pd.read_excel(origen + "/" + crudo,
                                     sheet_name=nombreSheet)
                dato.to_csv(destino + "/" + date.today().strftime("%d-%m-%Y") +
                            " CSV " + crudoSinFormato + ".csv", index=False)

    except:
        print("No procesado Excel to CSV")
        
# requerimos permisos para conectarnos con Google Storage
def csvToStorage():
    from google.cloud import storage
    import io
    from io import BytesIO
    storage_client = storage.Client(
        r"C:\Users\diazda\Desktop\Nueva carpeta\Daniel\PythonETL\creds.json")
    Bucket_name = "13c607f9-3658-4c38-ac1a-2a3e5348e2da"

    list_Bucket = storage_client.get_bucket(Bucket_name)

    files = list(list_Buckets.list_blobs(prefix=''))

    for name in files:
        print(name.name)
#Fin


def sqlToCSV(querry, destino, nombre=date.today().strftime("%d-%m-%Y"), DB="arcor2bis001"):
    try:
        import pyodbc
        sql_conn = pyodbc.connect("""DRIVER={ODBC Driver 17 for SQL Server};
                            SERVER=""" + DB + """;
                            Trusted_Connection=yes""")

        df = pd.read_sql(querry, sql_conn)
        if nombre.rsplit(".", 1)[-1] != 'csv':
            nombre += ".csv"
        df.to_csv(destino + "/" + nombre, index=False)
        sql_conn.close()
    except:
        print("No procesado Sql To CSV")


print("hola mundo")