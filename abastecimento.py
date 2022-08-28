import sys
import pandas as pd
import os
import timeit
import time
from datetime import date, datetime
from datetime import timedelta
from loguru import logger
import numpy as np

class Preencher_Carga:

    def __init__(self) -> None:
        logger.success('Iniciando...')
        self.inicio = timeit.default_timer()

        self.bases = os.getcwd() + "\\Base\\"
        self.nomeArquivo = ['CARTEIRA', 'Fechamento', 'Frota', 'Lista', 'A2J315']

        try:
            self.carteira, self.fechamento, self.frota, self.lista, self.suprimentos = self.listar_bases(self.bases, self.nomeArquivo)
        except Exception as e:
            logger.error('Falha em obter dados >> %s' % str(e))
            self.sair()

        self.destino = os.getcwd() + "\\Resultado\\"

    def exemple():
        # self.lDataInicial = (datetime.now()- timedelta(days=1)).strftime('%d-%m-%Y')
        # lista_romaneio = plan['Nro. Romaneio'].to_list() listar itens

        # df[['Cd', 'Comp', 'Item']] = df['Cd    Comp  Item'].str.split('  ', expand=True)
        # df_carteira.to_excel(self.destino + 'Base_carteira.xlsx', index=False)
        
        # df['Item'].fillna('-', inplace=True)

        # remove_filiais = df.loc[
        #     (df['Cd'].str.startswith(('0125', '1088', '1445', '1475', '1522', '1668', '1760', '1850', '1876',
        #                             '1888', '3200')))]

        # df.drop(remove_filiais.index, axis=0, inplace=True, errors='ignore')

        # df.replace({'Cd': {'0014': '1401'}}, inplace=True)

        # print("Hello to the {} {}".format(var2,var1))
        # print("Hello to the %s %d " %(var2,var1))
        
        # df['combo'] = np.select([df.mobile == 'mobile', df.tablet == 'tablet'], 
        #                         ['mobile', 'tablet'], 
        #                         default='other')
        # # or 
        # df['combo'] = np.where(df.mobile == 'mobile', 'mobile', 
        #                     np.where(df.tablet == 'tablet', 'tablet', 'other'))
        # def func(row):
        #     if row['PEDIDO DE VENDA'] >0:
        #         return '0.PV'
        #     elif row['tablet'] == 'tablet':
        #         return 'tablet'
        #     else:
        #         return 'other'

        # df_carteira['PRIORIDADE'] = df_carteira.apply(func, axis=1)
        # data_limite = datetime.strptime(data, '%d.%m.%Y').date()
        pass
    
    def sair(self):
        fim = timeit.default_timer()
        logger.critical('Finalizado... %ds' %(fim - self.inicio))
        sys.exit()

    def start(self):

        try:
            df_carteira = self.dados_carteira()
        except Exception as e:
            logger.warning('Falha em obter dados da Carteira >> %s' % str(e))
            self.sair
            
        try:    
            df_fechamento, df_frota, df_suprimentos = self.dados_auxiliar()
        except Exception as e:
            logger.warning('Falha em obter dados auxiliares >> %s' % str(e))
            self.sair

        self.tratar_dados(df_carteira, df_fechamento, df_frota, df_suprimentos)

        fim = timeit.default_timer()
        logger.success('Finalizado... %ds' %(fim - self.inicio))
        
    def dados_carteira(self):
        df_carteira = pd.read_csv(self.carteira, sep=";", header=0, encoding='latin-1')

        df_reordena = ['TIPO DE ENTRADA DO ITEM', 'TIPO PEDIDO', 
            'PEDIDO DE VENDA', 'PEDIDO', 'FILIAL ENTREGA', 'FILIAL DESTINO', 'MUNICIPIO', 'UF', 'TIPO ITEM', 'SITUACAO', 
            'SETOR', 'MERCADORIA', 'DESCRICAO', 'QTDE', 'CUBAGEM TOTAL', 'CUSTO MEDIO TOTAL', 
            'ESTOQ.FIL', 'DATA ENTRADA', 'DT CARGA PTO', 'CARGA PTO', 'TIPO DE CARGA', 
            'CARGA ENTREGA', 'BOX', 'DT.INCLUSAO CARGA.ETG', 'STATUS DA CARGA']
        df_carteira = self.reordenarColunas(df_carteira, df_reordena)

        df_carteira = df_carteira.replace({'CUBAGEM TOTAL': ',', 'CUSTO MEDIO TOTAL': ','}, value='.', regex=True)
        
        altera_coluna = {'STATUS DA CARGA': str, 
            'FILIAL DESTINO': str, 'DT CARGA PTO': str, 
            'DATA ENTRADA': str, 'TIPO PEDIDO': str,
            'QTDE': int, 'CUBAGEM TOTAL': float, 'CUSTO MEDIO TOTAL': float    
        }
        df_carteira = self.alterarTipo(df_carteira, altera_coluna)

        filtro = (df_carteira['STATUS DA CARGA'].str.startswith(('AGUARD. NOTA', 'TRANSITO')) | df_carteira['TIPO PEDIDO'].str.startswith(('TE', 'TP')))
        df_carteira = self.droparLinhas(df_carteira, filtro)

        df_carteira['CHIP'] = np.select(
            [(df_carteira['DESCRICAO'].str.contains('CHIP', na=False) 
                & ~df_carteira['DESCRICAO'].str.contains('CEL', na=False))]
            , ['Sim'], 'Não')

        df_carteira['DD Aging'] = (pd.to_datetime(date.today()) - pd.to_datetime(df_carteira['DATA ENTRADA'], format="%d.%m.%Y")).dt.days

        df_carteira['CHAVE'] = df_carteira['FILIAL DESTINO'] + '-' + df_carteira['DT CARGA PTO']

        df_carteira = self.definirPrioridade(df_carteira)
        
        df_carteira = self.agingEmCarteira(df_carteira)
        
        return df_carteira

    def dados_auxiliar(self):
        df_fechamento = pd.read_excel(self.fechamento)
        df_frota = pd.read_excel(self.frota, header=1)
        # df_lista = pd.read_excel(self.lista)
        df_suprimentos = pd.read_csv(self.suprimentos, sep=";", header=0, encoding='latin-1')
        
        df_reordena = ['CLUSTER', 'DESTINO', 'GH', 'FECHAMENTO 1200', 'DIA ENTREGA LOJA', 'FREQ', 'POSTO DE ASSIST', 
            'TRANSIT POINT', 'OBSERVAÇÃO', 'TIPOS DE VEICULOS (PLANO)', 'TIPOS DE VEICULOS (CAPACIDADE LOJA)']
        df_fechamento = self.reordenarColunas(df_fechamento, df_reordena)

        altera_coluna = {'DESTINO': str, 'GH': int,'FREQ': int, 'POSTO DE ASSIST': float, 'TRANSIT POINT': float}
        df_fechamento = self.alterarTipo(df_fechamento, altera_coluna)

        # df_reordena = ['Cluster', 'FILIAL', 'GH', 'TRANSP.', 'Freq.', 'HORÁRIO CARREGAMENTO', 'TRANSPORTADOR', 'OBSERVAÇÃO']
        # df_lista = self.reordenarColunas(df_lista, df_reordena)

        df_reordena = ['FIL PTO', 'DT CARGA', 'CUBAGEM']
        df_suprimentos = self.reordenarColunas(df_suprimentos, df_reordena)
        df_suprimentos = self.renomearColunas(df_suprimentos, {'CUBAGEM':'SUPR. CUB'})

        df_suprimentos = df_suprimentos.replace({'SUPR. CUB': ','}, value='.', regex=True)

        altera_coluna = {'FIL PTO': str, 'DT CARGA': str, 'SUPR. CUB': float}
        df_suprimentos = self.alterarTipo(df_suprimentos, altera_coluna)

        df_suprimentos['CHAVE'] = df_suprimentos['FIL PTO'] + "-" + df_suprimentos['DT CARGA']

        return df_fechamento, df_frota, df_suprimentos
        
    def tratar_dados(self, df_carteira, df_fechamento, df_frota, df_suprimentos):
        df_carteira = pd.merge(df_carteira, df_fechamento,
            how='left', left_on='FILIAL DESTINO', right_on='DESTINO')\
            .drop(columns = ['DESTINO', 'DIA ENTREGA LOJA', 'DD Aging'])

        df_carteira = pd.merge(df_carteira, df_suprimentos,
            how='left', on='CHAVE')\
            .drop(columns = ['CHAVE', 'FIL PTO', 'DT CARGA'])
        
        df_cluster = pd.pivot_table(df_carteira, values=['QTDE', 'CUBAGEM TOTAL', 'CUSTO MEDIO TOTAL'], 
            index= ['CLUSTER', 'FILIAL DESTINO', 'GH', 'FECHAMENTO 1200', 'FREQ'], 
            aggfunc={'QTDE' : np.sum, 'CUBAGEM TOTAL': np.sum, 'CUSTO MEDIO TOTAL': np.sum},
            fill_value=0)

        df_carteira.to_csv(self.destino + 'Base_carteira.csv', index=False, sep=";", encoding='latin-1')
        df_cluster.to_csv(self.destino + 'Base_cluster.csv', sep=";", encoding='latin-1')

    def definirPrioridade(self, df):
        conditions = [
            (df['PEDIDO DE VENDA'] == 0), 
            (df['TIPO DE ENTRADA DO ITEM'].str.strip() == 'REQ.SUPPLY'),
            (df['SETOR'].str.strip() == 'TELEFONIA CELULAR'),
            (df['SETOR'].str.strip().isin(['TVS', 'TABLETS', 'INFORMATICA']))
        ]
        result = ['0.PV', '1.Lista Supply', '2.Telefonia', '3.Tecnologia']

        df['PRIORIDADE'] = np.select(conditions, result, ['4.Aging'])

        return df

    def agingEmCarteira(self, df):
        conditions = [
            (df['DD Aging'] < 15), 
            (df['DD Aging'] <= 20),
            (df['DD Aging'] <= 25)
        ]
        result = [df['DD Aging'], '15 a 20', '21 a 25']

        df['Aging DD'] = np.select(conditions, result, ['>25'])

        return df

    def listar_bases(self, diretorio, nomeArquivo):
        l_arquivos = os.listdir(diretorio)
        l_datas = []
        time.sleep(1)
        for arquivo in l_arquivos:
            if any(nome in arquivo for nome in nomeArquivo):
                data = os.path.getmtime(os.path.join(os.path.realpath(diretorio), arquivo))
                l_datas.append((data, arquivo))
        l_datas.sort()

        for arquivo in l_datas:
            if nomeArquivo[0] in arquivo[1]: carteira = os.path.join(os.path.realpath(diretorio), arquivo[1])
            if nomeArquivo[1] in arquivo[1]: fechamento = os.path.join(os.path.realpath(diretorio), arquivo[1])
            if nomeArquivo[2] in arquivo[1]: frota = os.path.join(os.path.realpath(diretorio), arquivo[1])
            if nomeArquivo[3] in arquivo[1]: lista = os.path.join(os.path.realpath(diretorio), arquivo[1])
            if nomeArquivo[4] in arquivo[1]: suprimentos = os.path.join(os.path.realpath(diretorio), arquivo[1])
            
        return carteira, fechamento, frota, lista, suprimentos

    def reordenarColunas(self, df, lista):
        df = df.reindex(
            columns=lista)
        return df

    def renomearColunas(self, df, lista):
        df.rename(columns=lista, inplace=True, errors='ignore')
        return df

    def ordenarLinhas(self, df, lista):
        df = df.sort_values(by=lista, inplace=True)
        return df

    def droparLinhas(self, df, filtro):
        filtro_drop = df.loc[filtro] 
        df.drop(filtro_drop.index, axis=0, inplace=True, errors='ignore')
        return df

    def alterarTipo(self, df, tipos):
        df = df.astype(tipos, errors='ignore')
        return df

if __name__ == '__main__':
    executa = Preencher_Carga()
    executa.start()