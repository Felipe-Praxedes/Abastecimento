import pandas as pd
import numpy as np
import os

df_frota = pd.read_excel(
    r'C:\Users\2902374072\Desktop\Projetos\Dev\Python\Abastecimento\Base\Frota_Disp(SP).xlsx', header=1)

df_frota.reset_index()

destino = os.getcwd() + "\\Resultado\\"

l_data= []
for index, row in df_frota.iterrows():
    x = 0
    for c in df_frota.columns:
        try:
            col: str = c.replace('m³', '')[(c.index('a '))+1:10].strip()
        except:
            col: str = c.replace('m³', '').strip()
            
        if 'Transp' not in col and 'Cam' not in row[0] and 'TOTAL' not in row[0]:
            tipo = 'Local'
            if 'POLO' in row[0]: tipo = 'Polo' 
            l_data.append({'Transportadora': row[0], 'Tipo': tipo,'m³': col, 'Qtde': row[x]})
        x += 1
df = pd.DataFrame(l_data, columns=['Transportadora', 'Tipo', 'm³', 'Qtde'])

df.to_csv(destino + 'Base_teste.csv', index=False,sep=";", encoding='latin-1')