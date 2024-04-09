import csv


def open_csv():
    try:
        with open('src/localidades.csv', newline='') as archivo_csv:
            lector_csv = csv.reader(archivo_csv, delimiter=',', quotechar='"')
            return list(lector_csv)
    except FileNotFoundError as e:
        print(f'No se pudo abrir el archivo. Error: {e}')
        return []

