import pandas as pd
import numpy as np
import sys

# definição de padrão da chamada do script processar dados
# python3 processar_dados.py <sourceFile> <CNES> <destinationFIle>

def processar_dados_csv(csv_file_path: str, output_file_path: str, cnes: int):
    """Função que processa o arquivo CSV gerado."""
    # Carregando os dados
    df_main = pd.read_csv(csv_file_path, encoding='latin1', low_memory=False)
    df_proc = pd.read_csv("dadosprocedimentos.csv")
    df_tunep = pd.read_csv("tabela_tunep_mais_origem.csv", encoding='latin1')


    # Filtrando por CNES
    df_cnes = df_main[df_main['PA_CODUNI'] == cnes]  # Puxar CNES de algum local
    df_filt = df_cnes[['PA_CODUNI', 'PA_CMP', 'PA_PROC_ID', 'PA_QTDAPR', 'PA_VALPRO']]

    print("DF cnes")
    print(df_cnes)

    # Agrupando e somando por quantidade
    df_sum = df_filt.groupby(['PA_CODUNI', 'PA_CMP', 'PA_PROC_ID', 'PA_VALPRO'], as_index=False).agg({'PA_QTDAPR': 'sum'})
    df_sum = df_sum.sort_values(by='PA_PROC_ID')

    # Convertendo tipos
    df_sum['PA_PROC_ID'] = df_sum['PA_PROC_ID'].astype(str)
    df_proc['CO_PROCEDIMENTO'] = df_proc['CO_PROCEDIMENTO'].astype(str)

    # Merge com descrição dos procedimentos
    df_desc = pd.merge(df_sum, df_proc, left_on='PA_PROC_ID', right_on='CO_PROCEDIMENTO', how='left')
    df_desc = df_desc[['CO_PROCEDIMENTO', 'NO_PROCEDIMENTO', 'PA_CMP', 'PA_VALPRO', 'PA_QTDAPR']]

    # Calculando IVR
    df_desc["IVR"] = df_desc["PA_VALPRO"] * df_desc["PA_QTDAPR"] * 0.5

    # Merge com tabela TUNEP
    df_desc['CO_PROCEDIMENTO'] = df_desc['CO_PROCEDIMENTO'].astype(str)
    df_tunep['CO_PROCEDIMENTO'] = df_tunep['CO_PROCEDIMENTO'].astype(str)
    df_desc = pd.merge(df_desc, df_tunep, on='CO_PROCEDIMENTO', how='left').fillna(0)

    # Convertendo valores para comparação
    df_desc['ValorTUNEP'] = pd.to_numeric(df_desc['ValorTUNEP'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
    comp_valor = df_desc['PA_QTDAPR'] * df_desc['ValorTUNEP']
    df_desc['TUNEP'] = np.where(comp_valor > df_desc['IVR'], comp_valor, df_desc['IVR'])

    # Selecionando colunas finais
    df_final = df_desc[['CO_PROCEDIMENTO', 'NO_PROCEDIMENTO', 'PA_CMP', 'PA_VALPRO', 'PA_QTDAPR', 'IVR', 'ValorTUNEP', 'TUNEP']]
    print(df_final)

    # Salvar o resultado final em um novo arquivo CSV
    output_csv_path = csv_file_path.replace(".csv", "_processado.csv")
    df_final.to_csv(output_csv_path, index=False, encoding='latin1')
    print(f"Resultado salvo em: {output_csv_path}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Uso: python processar_dados.py <caminho_do_csv>")
        exit(1)

    csv_file_path = sys.argv[1]
    output_file_path = sys.argv[3]
    try:
        cnes = int(sys.argv[2])
    except:
        print(f"cnes inserido é invalido: {sys.argv[2]}")
        exit(1)

    processar_dados_csv(csv_file_path, output_file_path, cnes)
