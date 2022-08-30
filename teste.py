import pandas as pd
import numpy as np
import os

destino = os.getcwd() + "\\Resultado\\"
df = pd.read_csv(r'C:\Users\2902374072\Desktop\Projetos\Dev\Python\Abastecimento\Base\[DRP]_GERENCIAMENTO_DE_ESTOQUE_FÍSICO_20220829182347.csv',
     sep=";", header=0, encoding='latin-1', dtype=str)

firstColumn = df.columns[0]

df_reordena = [firstColumn, 'FILIAL', 'CLASSIFICACAO', 'DDV_FUTURO', 'DDV_SO', 'SINALIZADOR']
df = df.reindex(columns=df_reordena)

# df['FILIAL'] = df['FILIAL'].replace('0021_0', '', regex=True)
df['FILIAL'] = df['FILIAL'].str[-4:]
df['CHAVE_DDE'] = df['FILIAL'] + '-' + df[firstColumn]

df.to_csv(destino + 'Base_teste.csv', index=False, sep=";", encoding='latin-1')