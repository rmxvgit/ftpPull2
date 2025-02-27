import pandas as pd
import math as m
import time as t

def get_day():
    date = t.localtime()
    mes_str = ""
    if (date.tm_mon >= 10):
        mes_str = str(date.tm_mon)
    else:
        mes_str = f"0{date.tm_mon}"

    if (date.tm_mday >= 10):
        day_str = str(date.tm_mon)
    else:
        day_str = f"0{date.tm_mon}"

    return f"{day_str}/{mes_str}/{date.tm_year}"

def CarregaSelic() -> str[float]:
    today_str = get_day()
    url_bcb = f" https://api.bcb.gov.br/dados/serie/bcdata.sgs.4390/dados?formato=csv&dataInicial=01/01/2021&dataFinal={today_str}"
    selic = pd.read_csv(url_bcb, sep=";")
    selic['valor'] = selic['valor'].str.replace(",", ".").astype(float)
    selic['valor'] = (selic['valor']/100) + 1
    return selic['valor'].__array__()

def CalculaSelic(data_inicio: dict[str, int], selic_array: list[float]):
    inicio_da_selic = {'year': 2021, 'month': 12}
    posicao_do_inicio_na_array = (data_inicio['year'] - inicio_da_selic['year'])*12 + data_inicio['month'] - inicio_da_selic['month']
    if posicao_do_inicio_na_array < 0:
        posicao_do_inicio_na_array = 0

    return m.prod(selic_array[posicao_do_inicio_na_array:])

print(CarregaSelic())