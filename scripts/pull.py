import ftplib as ftp
import pandas as pd
import sys
import os
import sys
from multiprocessing import Pool

import pandas as pd
from dbfread import DBF
from fpdf import FPDF
from tempo import Tdata
import processar_dados_sia
import processar_dados_sih

# python3 pull.py SIA RS 01-24 01-24 2248328
# TODO: criar forma de conferir se os arquivos foram baixados na íntegra

searchDirs = {
    'SIA': ["/dissemin/publicos/SIASUS/199407_200712/Dados", "/dissemin/publicos/SIASUS/200801_/Dados"],
    'SIH': ["/dissemin/publicos/SIHSUS/199201_200712/Dados", "/dissemin/publicos/SIHSUS/200801_/Dados"]
}

search_prefix = {
    'SIA': 'PA',
    'SIH': 'SP'
}

# padrão de chamada do programa:
# python pull.py <SIA/SIH> <estado> <data-inicio> <data-fim> <CNES>

def main():
    args = sys.argv[1:]
    if not validate_args(args): return
    data_inicio = Tdata.str_to_data(args[2])
    data_fim = Tdata.str_to_data(args[3])

    print(args)

    get_and_process_data(args[1], data_inicio, data_fim, args[0], args[4])


def validate_args(args: list[str]) -> bool:
    if len(args) != 5:
        print("Número de argumentos fornecidos é inválido")
        return False
    if args[0] not in ['SIA', 'SIH']:
        print("Argumento inválido:", args[0])
        return False
    try:
        data_inicio = Tdata.str_to_data(args[2])
        data_fim = Tdata.str_to_data(args[3])
        if data_fim < data_inicio:
            print("Data de início maior que data de fim")
            return False
    except ValueError:
        return False
    return True

def find_files_of_interest(estado: str, data_inicio: Tdata, data_fim: Tdata, sih_sia: str) -> list[str]:
    files = []
    search_dirs = searchDirs[sih_sia]
    ftp_client = ftp.FTP("ftp.datasus.gov.br")
    ftp_client.login()

    for dir in search_dirs:
        print(f"{dir} <---- vasculhando diretório")
        def append_to_file(file: str):
            file = file.split(' ')[-1]
            dateString =  file[6:8] + "-" + file[4:6]
            try: date = Tdata.str_to_data(dateString)
            except: return

            if file[0:2] != search_prefix[sih_sia] or estado != file[2:4] or date < data_inicio or data_fim < date:
                return
            files.append(dir + "/" + file)

        ftp_client.cwd(dir)
        ftp_client.retrlines("LIST", append_to_file)
    ftp_client.quit()
    return files

def get_and_process_data(estado: str, data_inicio: Tdata, data_fim: Tdata, sia_sih: str, cnes: str):
    files_of_interest = find_files_of_interest(estado, data_inicio, data_fim, sia_sih)
    print(f"Arquivos a serem baixados:\n{files_of_interest}")

    create_storage_folders()

    with Pool(10) as p:
        p.map(dowload_e_processamento, [[file, cnes] for file in files_of_interest])

    sys.exit()
    unite_files()

    #TODO: remover os arquivos baixados

    #print("gerando pdf")
    #create_pdf_from_csv("../finalcsvs/resultado_final.csv", "../finalcsvs/resultado_final.pdf")


def create_storage_folders() -> None:
    try:
        os.makedirs("../downloads")
    except:
        pass
    try:
        os.makedirs("../dbfs")
    except:
        pass
    try:
        os.makedirs("../csvs")
    except:
        pass
    try:
        os.makedirs("../finalcsvs")
    except:
        pass


def unite_files():
    print("unindo arquivos .csv")
    laudo_files = os.listdir("../finalcsvs")
    composite_file = os.open("../finalcsvs/resultado_final.csv", os.O_CREAT | os.O_WRONLY)
    for laudo_file in laudo_files:
        simple_file = os.open(f"../finalcsvs/{laudo_file}", os.O_RDONLY)
        os.write(composite_file, os.read(simple_file, os.fstat(simple_file).st_size))
        os.close(simple_file)
    os.close(composite_file)


def file_was_already_dowloaded(file_name: str) -> bool:
    return os.path.exists(f"../downloads/{file_name}")


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


def create_pdf_from_csv(source_file_path: str, output_file_path: str):
    csv = pd.read_csv(source_file_path, encoding='latin-1')
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('helvetica', size=8)
    pdf.cell(200, 10, txt="Laudo Do AvogaSUS", align='C')
    pdf.ln()
    pdf.set_font('helvetica', size=4)
    for collumn in list(csv.columns):
        pdf.cell(24, 10, txt=str(collumn)[:20], border=1)
    pdf.ln()
    for index, row in csv.iterrows():
        for item in row:
            pdf.cell(24, 10, txt=str(item)[:20], border=1)
        pdf.ln()

    pdf.output(output_file_path)


def dowload_e_processamento(file_and_cnes: list[str]):
    file = file_and_cnes[0]
    cnes = file_and_cnes[1]
    fileName = os.path.split(file)[1]
    start_time = Tdata.str_to_data(f"{fileName[6:8]}-{fileName[4:6]}")
    if not file_was_already_dowloaded(fileName):
        print(f"Dowload de {file}...")
        dowload_from_ftp("ftp.datasus.gov.br", file, "../downloads/")

    print("Conversão para dbf...")
    os.system(f"../exes/blast-dbf ../downloads/{fileName} ../dbfs/{fileName[:-4]}.dbf")

    print("Conversão para csv...")
    os.system(f"../exes/DBF2CSV ../dbfs/{fileName[:-4]}.dbf ../csvs/{fileName[:-4]}.csv {cnes} {sys.argv[1]}")

    print("Processando dados do csv por cnes...")
    if (sys.argv[1] == "SIA"):
        processar_dados_sia.processar_dados_csv(f"../csvs/{fileName[:-4]}.csv", f"../finalcsvs/{fileName[:-4]}.csv", start_time, Tdata.current_data())
    else:
        processar_dados_sih.processar_dados_csv(f"../csvs/{fileName[:-4]}.csv", f"../finalcsvs/{fileName[:-4]}.csv", start_time, Tdata.current_data())


main()
