import ftplib as ftp
from typing import Text
from fpdf import FPDF
import csv
from dbfread import DBF
from fpdf.text_region import XPos, YPos
import pandas as pd
import sys
import os

from pandas.io.parsers.readers import read_csv

searchDirs = {
    'SIA': ["/dissemin/publicos/SIASUS/199407_200712/Dados", "/dissemin/publicos/SIASUS/200801_/Dados"],
    'SIH': ["/dissemin/publicos/SIHSUS/199201_200712/Dados", "/dissemin/publicos/SIHSUS/200801_/Dados"]
}

search_prefix = {
    'SIA': 'PA',
    'SIH': 'RD'
}

# padrão de chamada do programa:
# python pull.py <SIA/SIH> <estado> <data-inicio> <data-fim> <CNES>

def main():
    args = sys.argv[1:]
    if not validate_args(args): return
    data_inicio = to_time(args[2])
    data_fim = to_time(args[3])

    print(args)

    if args[0] == 'SIA':
        get_and_process_data(args[1], data_inicio, data_fim, 'SIA', args[4])
    elif args[0] == 'SIH':
        get_and_process_data(args[1], data_inicio, data_fim, 'SIH', args[4])
    else:
        print("Argumento inválido:", args[0])

def validate_args(args: list[str]) -> bool:
    if len(args) != 5:
        print("Número de argumentos inválido")
        return False
    if args[0] not in ['SIA', 'SIH']:
        print("Argumento inválido:", args[0])
        return False
    try:
        data_inicio = to_time(args[2])
        data_fim = to_time(args[3])
        if is_date_less(data_fim, data_inicio):
            print("Data de início maior que data de fim")
            return False
    except ValueError:
        return False
    return True

def is_date_less(date1: dict[str, int], date2: dict[str, int]) -> bool:
    return date1['year'] < date2['year'] or (date1['year'] == date2['year'] and date1['month'] < date2['month'])

def find_files_of_interest(estado: str, data_inicio: dict[str, int], data_fim: dict[str, int], sih_sia: str):
    files = []
    search_dirs = searchDirs[sih_sia]
    ftp_client = ftp.FTP("ftp.datasus.gov.br")
    ftp_client.login()
    for dir in search_dirs:
        print(dir)
        def append_to_file(file: str):
                file = file.split(' ')[-1]
                dateString =  file[6:8] + "-" + file[4:6]
                try: date = to_time(dateString)
                except: return

                if file[0:2] != search_prefix[sih_sia] or estado != file[2:4] or is_date_less(date, data_inicio) or is_date_less(data_fim, date):
                    return
                files.append(dir + "/" + file)

        ftp_client.cwd(dir)
        ftp_client.retrlines("LIST", append_to_file)
    ftp_client.quit()
    return files

def get_and_process_data(estado: str, data_inicio: dict[str, int], data_fim: dict[str, int], sia_sih: str, cnes: str):
    files_of_interest = find_files_of_interest(estado, data_inicio, data_fim, sia_sih)
    print("Arquivos a serem baixados:")
    print(files_of_interest)

    try:
        os.makedirs("downloads")
        os.makedirs("dbfs")
        os.makedirs("csvs")
        os.makedirs("finalcsvs")
    except:
        pass

    for file in files_of_interest:
        fileName = os.path.split(file)[1]
        if not file_was_already_dowloaded(fileName):
            print(f"Dowload de {file}...")
            dowload_from_ftp("ftp.datasus.gov.br", file, f"{os.curdir}/downloads/")

        if not file_was_already_converted_to_dbf(f"{fileName[:-4]}.dbf"):
            print("Conversão para dbf...")
            os.system(f"./blast-dbf ./downloads/{fileName} ./dbfs/{fileName[:-4]}.dbf" )

        print("Conversão para csv...")
        dbf_to_csv(f"./dbfs/{fileName[:-4]}.dbf", f"./csvs/{fileName[:-4]}.csv")
        print("Processando dados do csv por cnes...")
        os.system(f"python3 processar_dados.py ./csvs/{fileName[:-4]}.csv {cnes}")

def file_was_already_dowloaded(file_name: str) -> bool:
    return os.path.exists(f"./downloads/{file_name}")

def file_was_already_converted_to_dbf(file_name: str) -> bool:
    return os.path.exists(f"./dbfs/{file_name}")

def to_time(data: str) -> dict[str, int]:
    month_year = [int(x) for x in data.split('-')]
    if month_year[1] < 1900:
        if month_year[1] > 70:
            month_year[1] += 1900
        else:
            month_year[1] += 2000
    return {'month': month_year[0], 'year': month_year[1]}

def dowload_from_ftp(ftp_server: str, remote_path: str, local_dir: str):
    try:
        print("Iniciando o download de", remote_path)
        remote_dir, remote_file = os.path.split(remote_path)
        local_file = os.path.join(local_dir, remote_file)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        ftp_client = ftp.FTP(ftp_server)
        ftp_client.login()
        ftp_client.cwd(remote_dir)
        with open(local_file, 'wb') as file:
            ftp_client.retrbinary('RETR ' + remote_file, file.write)
        ftp_client.quit()
        print(f"Download de {remote_file} concluído com sucesso.")
        return
    except:
        print("Erro ao fazer download", remote_path)
        return

def dbf_to_csv(dbf_table_pth: str, csv_output_path: str):#Input a dbf, output a csv, same name, same path, except extension
    table = DBF(dbf_table_pth)# table variable is a DBF object
    with open(csv_output_path, 'w', newline = '') as f:# create a csv file, fill it with dbf content
        writer = csv.writer(f)
        writer.writerow(table.field_names)# write the column name
        for record in table:# write the rows
            writer.writerow(list(record.values()))

def create_pdf_from_csv(source_file_path: str, output_file_path: str):
    csv = pd.read_csv(source_file_path)
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('helvetica', size=12)
    pdf.cell(200, 10, text="Laudo Do AvogaSUS", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
    pdf.set_font('helvetica', size=8)
    for collumn in list(csv.columns):
        pdf.cell(18, 10, text=str(collumn), border=1)
    pdf.ln()
    for index, row in csv.iterrows():
        for item in row:
            pdf.cell(18, 10, text=str(item), border=1)
        pdf.ln()

    pdf.output(output_file_path)

main()
