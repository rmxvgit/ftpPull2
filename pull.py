import ftplib as ftp
import csv
from dbfread import DBF
import sys
import os

# padrão de chamada do programa:
# python pull.py <SIA/SIH> <estado> <data-inicio> <data-fim>

def main():
    args = sys.argv[1:]
    if not validate_args(args): return
    data_inicio = to_time(args[2])
    data_fim = to_time(args[3])

    print(args)

    if args[0] == 'SIA':
        sia_pull(args[1], data_inicio, data_fim)
    elif args[0] == 'SIH':
        sih_pull(args[1], data_inicio, data_fim)
    else:
        print("Argumento inválido:", args[0])

def validate_args(args: list[str]) -> bool:
    if len(args) != 4:
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

def sia_pull(estado: str, data_inicio: dict[str, int], data_fim: dict[str, int]):
    files_of_interest = find_sia_files_of_interest(estado, data_inicio, data_fim)
    print("Arquivos a serem baixados:")
    print(files_of_interest)

    for file in files_of_interest:
        dowload_from_ftp("ftp.datasus.gov.br", file, os.curdir)
        fileName = os.path.split(file)[1]
        print("Conversão para dbf...")
        os.system(f"./blast-dbf {fileName} {fileName[:-4]}.dbf" )
        print("Conversão para csv...")
        dbf_to_csv(fileName[:-4] + ".dbf")

def is_date_less(date1: dict[str, int], date2: dict[str, int]) -> bool:
    return date1['year'] < date2['year'] or (date1['year'] == date2['year'] and date1['month'] < date2['month'])

def find_sia_files_of_interest(estado: str, data_inicio: dict[str, int], data_fim: dict[str, int]) -> list[str]:
    files = []
    search_dirs = ["/dissemin/publicos/SIASUS/199407_200712/Dados", "/dissemin/publicos/SIASUS/200801_/Dados"]
    ftp_client = ftp.FTP("ftp.datasus.gov.br")
    ftp_client.login()
    for dir in search_dirs:
        print(dir)
        def append_to_file(file: str):
                file = file.split(' ')[-1]
                dateString =  file[6:8] + "-" + file[4:6]
                try: date = to_time(dateString)
                except: return

                if file[0:2] != 'PA' or estado != file[2:4] or is_date_less(date, data_inicio) or is_date_less(data_fim, date):
                    return
                files.append(dir + "/" + file)

        ftp_client.cwd(dir)
        ftp_client.retrlines("LIST", append_to_file)
    ftp_client.quit()
    return files

def sih_pull(estado: str, data_inicio: dict[str, int], data_fim: dict[str, int]):
    files_of_interest = find_sih_files_of_interest(estado, data_inicio, data_fim)
    print("Arquivos a serem baixados:")
    print(files_of_interest)

    for file in files_of_interest:
        dowload_from_ftp("ftp.datasus.gov.br", file, os.curdir)
        fileName = os.path.split(file)[1]
        print("Conversão para dbf...")
        os.system(f"./blast-dbf {fileName} {fileName[:-4]}.dbf" )
        print("Conversão para csv...")
        dbf_to_csv(fileName[:-4] + ".dbf")

def find_sih_files_of_interest(estado: str, data_inicio: dict[str, int], data_fim: dict[str, int]) -> list[str]:
    files = []
    search_dirs = ["/dissemin/publicos/SIHSUS/199201_200712/Dados", "/dissemin/publicos/SIHSUS/200801_/Dados"]

    ftp_client = ftp.FTP("ftp.datasus.gov.br")
    ftp_client.login()

    for dir in search_dirs:
        def append_to_file(file: str): # sim, isso é uma declaração de função dentro de um for loop. E, sim, faz sentido.
                file = file.split(' ')[-1]
                dateString =  file[6:8] + "-" + file[4:6]
                try: date = to_time(dateString)
                except: return

                if file[0:2] != "RD" or estado != file[2:4] or is_date_less(date, data_inicio) or is_date_less(data_fim, date):
                    return
                files.append(dir + "/" + file)

        ftp_client.cwd(dir)
        ftp_client.retrlines("LIST", append_to_file)

    ftp_client.quit()
    return files

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

def dbf_to_csv(dbf_table_pth):#Input a dbf, output a csv, same name, same path, except extension
    csv_fn = dbf_table_pth[:-4]+ ".csv" #Set the csv file name
    table = DBF(dbf_table_pth)# table variable is a DBF object
    with open(csv_fn, 'w', newline = '') as f:# create a csv file, fill it with dbf content
        writer = csv.writer(f)
        writer.writerow(table.field_names)# write the column name
        for record in table:# write the rows
            writer.writerow(list(record.values()))

main()
