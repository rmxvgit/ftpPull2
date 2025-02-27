import pandas as pd


def CarregaSelic(data_inicial, data_final):
    url_bcb = f" https://api.bcb.gov.br/dados/serie/bcdata.sgs.4390/dados?formato=csv&dataInicial={data_inicial}&dataFinal={data_final}"
    selic = pd.read_csv(url_bcb, sep=";")
    return selic


serie = CarregaSelic("28/02/2023", "27/02/2025")

print(serie)