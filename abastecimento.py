from tkinter import messagebox as mb
from loguru import logger
from datetime import date, datetime
import pandas as pd
import numpy as np
import timeit
import os
import sys


class Preencher_Carga:

    def __init__(self) -> None:
        logger.success('Iniciando...')
        self.inicio = timeit.default_timer()

        self.bases = os.getcwd() + "\\Base\\"
        self.destino = os.getcwd() + "\\Resultado\\"
        self.nomeArquivo = ['CARTEIRA', 'Fechamento', 'Frota', 'Lista', 'A2J315', 'DRP']

        try:
            self.carteira, self.fechamento, self.frota, self.lista, self.suprimentos, self.ddeSupply = self.listarBases(
                self.bases, self.nomeArquivo)
        except Exception as e:
            logger.error('Falha em obter base dados >> %s' % str(e))
            self.sair()

    def exemple(self):
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

        # df[(df.a > 1) & (df.a < 3)].sum()

        # df = pd.DataFrame({'a': ['a', 'b', 'a', 'a', 'b', 'c', 'd']})
        # after = df.groupby('a').size()
        # >> after
        # a
        # a    3
        # b    2
        # c    1
        # d    1
        # dtype: int64

        # >> after[after > 2]
        # a
        # a    3
        # dtype: int64
        # print(df_dia_semana[df_dia_semana['ETG_QUA'] >=1 ])
        # df = df.filter(regex='CODIGO_ITEM|ITEM')
        # df['FILIAL'] = df['FILIAL'].replace('0021_0', '', regex=True)
        # total = df_carteira[df_carteira['CLUSTER'] == 'SPMTR266'].sum()[['CUBAGEM TOTAL', 'CUSTO MEDIO TOTAL', 'QTDE']]
        # print(total)
        # df_carteira = df_carteira[df_carteira['FILIAL DESTINO'] == '1402']
        # df_carteira.to_csv(self.destino + 'Base_dePara.csv', index=False, sep=";", encoding='latin-1')

        # print(df_carteira['CUBAGEM TOTAL'].sum())
        # df['CUSTO'] = df['CUSTO'].map('{:_.2f}'.format)
        # df = df.str.replace(
        #     {'QTDE': '.', 'CUBAGEM TOTAL': '.', 'CUSTO MEDIO TOTAL': '.', 
        #     'QTD_CLUSTER': '.', 'CUB_CLUSTER': '.', 'CUSTO_CLUSTER': '.',
        #     'QTD_FILIAL': '.', 'CUB_FILIAL': '.', 'CUSTO_FILIAL': '.'}, value=',', regex=True)

        # df = self.alterarTipo(df, str)
        # df["Rank"] = df[["SaleCount","TotalRevenue"]].apply(tuple,axis=1)\
        #      .rank(method='dense',ascending=False).astype(int)

        # df.sort_values("Rank")
        # col1 = df["SaleCount"].astype(str) 
        # col2 = df["TotalRevenue"].astype(str)
        # df['Rank'] = (col1+col2).astype(int).rank(method='dense', ascending=False).astype(int)
        # df.sort_values('Rank')
        pass

    def sair(self, msg=''):
        if msg != '':
            logger.error('Dados não encontrado: %s' % msg)
        fim = timeit.default_timer()
        logger.critical('Finalizado... %ds' % (fim - self.inicio))
        sys.exit()

    def start(self):
        try:
            df_carteira = self.dadosCarteira()
        except Exception as e:
            logger.warning('Falha em obter dados da Carteira >> %s' % str(e))
            self.sair()

        try:
            df_ddeSupply = self.estoqueLojaSupply()
        except Exception as e:
            logger.warning('Falha em obter dados do estoque Supply >> %s' % str(e))
            self.sair()

        try:
            df_fechamento, df_frota, df_suprimentos = self.dadosAuxiliar()
        except Exception as e:
            logger.warning('Falha em obter dados auxiliares >> %s' % str(e))
            self.sair()

        try:
            df_plano = self.fechamentoPlano(df_fechamento)
        except Exception as e:
            logger.warning('Falha em obter dados de fechamento >> %s' % str(e))
            self.sair

        try:
            df_frota = self.frotaDisponivel(df_frota)
        except:
            logger.warning('Falha em obter dados de frota disponível >> %s' % str(e))
            self.sair

        df_carteira = self.tratarDados(df_carteira, df_fechamento, df_plano, df_suprimentos, df_ddeSupply)

        self.gerarSaida(df_carteira)

        fim = timeit.default_timer()
        logger.success('Finalizado... %ds' % (fim - self.inicio))

    def dadosCarteira(self):
        df_carteira = pd.read_csv(self.carteira, sep=";", header=0, encoding='latin-1', dtype=str)

        df_reordena = ['TIPO DE ENTRADA DO ITEM', 'TIPO PEDIDO',
                       'PEDIDO DE VENDA', ' PEDIDO', 'FILIAL ENTREGA', 'FILIAL DESTINO', 'MUNICIPIO', 'UF', 'TIPO ITEM',
                       'SITUACAO',
                       'SETOR', 'MERCADORIA', 'DESCRICAO', 'QTDE', 'CUBAGEM TOTAL', 'CUSTO MEDIO TOTAL',
                       'ESTOQ.FIL', 'DATA ENTRADA', 'DT CARGA PTO', 'CARGA PTO', 'TIPO DE CARGA',
                       'CARGA ENTREGA', 'BOX', 'DT.INCLUSAO CARGA.ETG', 'STATUS DA CARGA']

        self.validarColunas(df_carteira, df_reordena)

        df_carteira = self.reordenarColunas(df_carteira, df_reordena)

        renomear_coluna = {' PEDIDO': 'PEDIDO', 'TIPO DE ENTRADA DO ITEM': 'TIPO ENTRADA', 'CUBAGEM TOTAL': 'CUB',
                           'CUSTO MEDIO TOTAL': 'CUSTO'}
        df_carteira = self.renomearColunas(df_carteira, renomear_coluna)

        filtro = (df_carteira['STATUS DA CARGA'].str.startswith(('AGUARD. NOTA', 'TRANSITO')) | df_carteira[
            'TIPO PEDIDO'].str.startswith(('TE', 'TP')))
        df_carteira = self.droparLinhas(df_carteira, filtro)

        df_carteira = df_carteira.replace({'CUB': ',', 'CUSTO': ','}, value='.', regex=True)
        altera_coluna = {'QTDE': int, 'CUB': float, 'CUSTO': float, 'MERCADORIA': int}
        df_carteira = self.alterarTipo(df_carteira, altera_coluna)
        df_carteira = self.alterarTipo(df_carteira, {'MERCADORIA': str})

        df_carteira['CHIP'] = np.select(
            [(df_carteira['DESCRICAO'].str.contains('CHIP', na=False)
              & ~df_carteira['DESCRICAO'].str.contains('CEL', na=False))]
            , ['Sim'], 'Não')

        df_carteira['DD Aging'] = \
            (pd.to_datetime(date.today()) - pd.to_datetime(df_carteira['DATA ENTRADA'], format="%d.%m.%Y")).dt.days

        df_carteira['CHAVE'] = df_carteira['FILIAL DESTINO'] + '-' + df_carteira['DT CARGA PTO']
        df_carteira['CHAVE_DDE'] = df_carteira['FILIAL DESTINO'] + '-' + df_carteira['MERCADORIA']

        df_carteira = self.agingEmCarteira(df_carteira)

        return df_carteira

    def estoqueLojaSupply(self):
        df = pd.read_csv(self.ddeSupply, sep=";", header=0, encoding='latin-1', dtype=str)

        firstColumn = df.columns[0]
        df_reordena = [firstColumn, 'FILIAL', 'CLASSIFICACAO', 'DDV_FUTURO', 'DDV_SO', 'SINALIZADOR']

        self.validarColunas(df, df_reordena)
        df = self.reordenarColunas(df, df_reordena)

        df['FILIAL'] = df['FILIAL'].str[-4:]
        df['CHAVE_DDE'] = df['FILIAL'] + '-' + df[firstColumn]
        df = df.drop(columns=[firstColumn, 'FILIAL'])

        return df

    def dadosAuxiliar(self):
        df_fechamento = pd.read_excel(self.fechamento)
        df_frota = pd.read_excel(self.frota, header=1)
        # df_lista = pd.read_excel(self.lista)
        df_suprimentos = pd.read_csv(self.suprimentos, sep=";", header=0, encoding='latin-1', dtype=str)

        df_reordena = ['CLUSTER', 'DESTINO', 'GH', 'FECHAMENTO 1200', 'DIA ENTREGA LOJA',
                       'SEG', 'TER', 'QUA', 'QUI', 'SEX', 'FREQ', 'SOMA PLANO', 'POSTO DE ASSIST', 'TRANSIT POINT',
                       'OBSERVAÇÃO', 'TIPOS DE VEICULOS (PLANO)', 'TIPOS DE VEICULOS (CAPACIDADE LOJA)']

        self.validarColunas(df_fechamento, df_reordena)

        df_fechamento = self.reordenarColunas(df_fechamento, df_reordena)

        altera_coluna = {'DESTINO': str, 'GH': int,
                         'SEG': float, 'TER': float, 'QUA': float, 'QUI': float, 'SEX': float, 'FREQ': float,
                         'SOMA PLANO': float,
                         'POSTO DE ASSIST': float, 'TRANSIT POINT': float}
        df_fechamento = self.alterarTipo(df_fechamento, altera_coluna)

        renomear_coluna = {'SEG': 'CUB SEG', 'TER': 'CUB TER', 'QUA': 'CUB QUA', 'QUI': 'CUB QUI', 'SEX': 'CUB SEX',
                           'SOMA PLANO': 'CUB SEMANA', 'POSTO DE ASSIST': 'CUB PA', 'TRANSIT POINT': 'CUB TP',
                           'TIPOS DE VEICULOS (PLANO)': 'VEICULO PLANO',
                           'TIPOS DE VEICULOS (CAPACIDADE LOJA)': 'VEICULO LOJA'}
        df_fechamento = self.renomearColunas(df_fechamento, renomear_coluna)

        df_fechamento['DESTINO'] = ('000' + df_fechamento['DESTINO']).str[-4:]

        # df_reordena = ['Cluster', 'FILIAL', 'GH', 'TRANSP.', 'Freq.', 'HORÁRIO CARREGAMENTO', 'TRANSPORTADOR', 'OBSERVAÇÃO']
        # df_lista = self.reordenarColunas(df_lista, df_reordena)

        df_reordena = ['FIL PTO', 'DT CARGA', 'CUBAGEM']
        self.validarColunas(df_suprimentos, df_reordena)

        df_suprimentos = self.reordenarColunas(df_suprimentos, df_reordena)
        df_suprimentos = self.renomearColunas(df_suprimentos, {'CUBAGEM': 'CUB SUPR'})

        df_suprimentos = df_suprimentos.replace({'CUB SUPR': ','}, value='.', regex=True)

        altera_coluna = {'FIL PTO': str, 'DT CARGA': str, 'CUB SUPR': float}
        df_suprimentos = self.alterarTipo(df_suprimentos, altera_coluna)

        df_suprimentos['FIL PTO'] = ('000' + df_suprimentos['FIL PTO']).str[-4:]
        df_suprimentos['CHAVE'] = df_suprimentos['FIL PTO'] + "-" + df_suprimentos['DT CARGA']

        return df_fechamento, df_frota, df_suprimentos

    def tratarDados(self, df_carteira, df_fechamento, df_plano, df_suprimentos, df_ddeSupply):
        df_carteira = pd.merge(df_carteira, df_fechamento,
                               how='left', left_on='FILIAL DESTINO', right_on='DESTINO') \
            .drop(columns=['DESTINO', 'DD Aging'])

        df_carteira['CLUSTER'].fillna('SEM CLUSTER', inplace=True)

        df_carteira = pd.merge(df_carteira, df_ddeSupply,
                               how='left', on='CHAVE_DDE') \
            .drop(columns=['CHAVE_DDE'])

        df_carteira = pd.merge(df_carteira, df_suprimentos,
                               how='left', on='CHAVE') \
            .drop(columns=['CHAVE', 'FIL PTO', 'DT CARGA'])

        df_carteira = pd.merge(df_carteira, df_plano,
                               how='left', on='CLUSTER')

        df_cluster, df_destino = self.agruparDados(df_carteira)

        df_carteira = pd.merge(df_carteira, df_cluster,
                               how='left', on='CLUSTER')

        df_carteira = pd.merge(df_carteira, df_destino,
                               how='left', on='FILIAL DESTINO')

        df_carteira = df_carteira.replace({'QTDE': ',', 'CUB': ',', 'CUSTO': ','}, value='.', regex=True)

        altera_coluna = {'STATUS DA CARGA': str,
                         'FILIAL DESTINO': str, 'DT CARGA PTO': str,
                         'DATA ENTRADA': str, 'TIPO PEDIDO': str,
                         'QTDE': int, 'CUB': float, 'CUSTO': float}
        df_carteira = self.alterarTipo(df_carteira, altera_coluna)

        df_carteira['DD ESCOAMENTO'] = df_carteira['CUB_FILIAL'] / df_carteira['CUB SEMANA'] * 7

        ordenar_coluna = ['RANK_CLUSTER', 'RANK_FILIAL', 'CUB', 'CUSTO', 'QTDE']
        df_carteira = self.ordenarLinhas(df_carteira, ordenar_coluna, False)

        df_carteira = self.definirPrioridade(df_carteira)

        return df_carteira

    def agruparDados(self, df_carteira):
        df_cluster = pd.pivot_table(df_carteira, values=['QTDE', 'CUB', 'CUSTO'],
                                    index=['CLUSTER'],
                                    aggfunc={'QTDE': np.sum, 'CUB': np.sum, 'CUSTO': np.sum},
                                    fill_value=0)
        df_cluster = self.renomearColunas(df_cluster,
                                          {'QTDE': 'QTD_CLUSTER', 'CUB': 'CUB_CLUSTER', 'CUSTO': 'CUSTO_CLUSTER'})

        df_cluster['RANK_CLUSTER'] = df_cluster['CUB_CLUSTER'].rank(na_option='bottom')

        df_destino = pd.pivot_table(df_carteira, values=['QTDE', 'CUB', 'CUSTO'],
                                    index=['FILIAL DESTINO'],
                                    aggfunc={'QTDE': np.sum, 'CUB': np.sum, 'CUSTO': np.sum},
                                    fill_value=0)
        df_destino = self.renomearColunas(df_destino,
                                          {'QTDE': 'QTD_FILIAL', 'CUB': 'CUB_FILIAL', 'CUSTO': 'CUSTO_FILIAL'})

        df_destino['RANK_FILIAL'] = df_destino['CUB_FILIAL'].rank(na_option='bottom')

        return df_cluster, df_destino

    def definirPrioridade(self, df):
        conditions = [
            (df['TIPO PEDIDO'].isin(['PV', 'RR'])),
            (df['TIPO ENTRADA'].str.strip() == 'REQ.SUPPLY'),
            (df['SETOR'].str.strip() == 'TELEFONIA CELULAR'),
            (df['SETOR'].str.strip().isin(['TVS', 'TABLETS', 'INFORMATICA'])),
            (df['SINALIZADOR'].isin(['0 - ESTOQUE ZERO', '1 - MUITO BAIXO'])),
            (df['DD ESCOAMENTO'] >= 7),
            (df['Aging DD'].isin(['8', '9', '10 a 15', '16 a 20', '21 a 25', '>25']))
        ]
        result = ['0.Pv', '1.Lista_Supply', '2.Telefonia', '3.Tecnologia', '4.Baixo_dde', '5.Escoamento_+7DD',
                  '6.Aging']

        df['PRIORIDADE'] = np.select(conditions, result, ['7.Normal'])

        return df

    def agingEmCarteira(self, df):
        conditions = [
            (df['DD Aging'] < 10),
            (df['DD Aging'] <= 15),
            (df['DD Aging'] <= 20),
            (df['DD Aging'] <= 25)
        ]
        result = [df['DD Aging'], '10 a 15', '16 a 20', '21 a 25']

        df['Aging DD'] = np.select(conditions, result, ['>25'])

        return df

    def fechamentoPlano(self, df_1):
        df_1 = pd.DataFrame(
            {'QTDE_DIN':
                 df_1.groupby(['CLUSTER', 'DIA ENTREGA LOJA'])['OBSERVAÇÃO'].nunique()}) \
            .reset_index()

        df_dia_semana = pd.DataFrame(
            {'ETG_TTL':
                 df_1.groupby('CLUSTER')['QTDE_DIN'].sum()}) \
            .reset_index()

        dia_semana = ['ETG_SEG', 'ETG_TER', 'ETG_QUA', 'ETG_QUI', 'ETG_SEX']
        for ds in dia_semana:
            df_ds = df_1[df_1['DIA ENTREGA LOJA'].str.contains(ds[-3:])]
            df_ds = pd.DataFrame(
                {'%s' % ds:
                     df_ds.groupby('CLUSTER')['QTDE_DIN'].sum()}) \
                .reset_index()
            df_dia_semana = pd.merge(df_dia_semana, df_ds, how='left', on='CLUSTER')

        altera_coluna = {'ETG_SEG': int, 'ETG_TER': int, 'ETG_QUA': int, 'ETG_QUI': int, 'ETG_SEX': int, 'ETG_TTL': int}
        df_dia_semana = self.alterarTipo(df_dia_semana, altera_coluna)

        df_dia_semana.fillna(0, inplace=True)

        return df_dia_semana

    def frotaDisponivel(self, df):
        l_data = []
        df.reset_index()
        for index, row in df.iterrows():
            x = 0
            for c in df.columns:
                try:
                    col: str = c.replace('m³', '')[(c.index('a ')) + 1:10].strip()
                except:
                    col: str = c.replace('m³', '').strip()

                if 'Transp' not in col and 'Cam' not in row[0] and 'TOTAL' not in row[0]:
                    tipo = 'Local'
                    if 'POLO' in row[0]: tipo = 'Polo'
                    l_data.append({'Transportadora': row[0], 'Tipo': tipo, 'm³': col, 'Qtde': row[x]})
                x += 1
        df = pd.DataFrame(l_data, columns=['Transportadora', 'Tipo', 'm³', 'Qtde'])

        df_veiculo = df  # Se quiser usar veiculo
        df = pd.pivot_table(df, values=['Qtde'],
                            index=['Tipo', 'm³'],
                            aggfunc={'Qtde': np.sum},
                            fill_value=0)

        return df

    def listarBases(self, diretorio, nomeArquivo):
        l_arquivos = os.listdir(diretorio)
        l_datas = []
        for arquivo in l_arquivos:
            if any(nome in arquivo for nome in nomeArquivo):
                data = os.path.getmtime(os.path.join(os.path.realpath(diretorio), arquivo))
                l_datas.append((data, arquivo))
        l_datas.sort()

        carteira = None
        fechamento = None
        frota = None
        lista = None
        suprimentos = None
        ddeSupply = None

        for arquivo in l_datas:
            if nomeArquivo[0] in arquivo[1]: carteira = os.path.join(os.path.realpath(diretorio), arquivo[1])
            if nomeArquivo[1] in arquivo[1]: fechamento = os.path.join(os.path.realpath(diretorio), arquivo[1])
            if nomeArquivo[2] in arquivo[1]: frota = os.path.join(os.path.realpath(diretorio), arquivo[1])
            if nomeArquivo[3] in arquivo[1]: lista = os.path.join(os.path.realpath(diretorio), arquivo[1])
            if nomeArquivo[4] in arquivo[1]: suprimentos = os.path.join(os.path.realpath(diretorio), arquivo[1])
            if nomeArquivo[5] in arquivo[1]: ddeSupply = os.path.join(os.path.realpath(diretorio), arquivo[1])

        if carteira is None: self.sair('Base da Carteira')
        if fechamento is None: self.sair('Base de Fechamento')
        if frota is None: self.sair('Base de Frota disponível')
        # if lista is None: self.sair('Lista')
        if suprimentos is None: self.sair('Base de Suprimentos')
        if ddeSupply is None: self.sair('Base DRP Supply')

        return carteira, fechamento, frota, lista, suprimentos, ddeSupply

    def reordenarColunas(self, df, lista):
        df = df.reindex(
            columns=lista)
        return df

    def renomearColunas(self, df, lista):
        df.rename(columns=lista, inplace=True, errors='ignore')
        return df

    def ordenarLinhas(self, df, lista, bool=True):
        df.sort_values(by=lista, inplace=True, ascending=bool, ignore_index=True)
        return df

    def droparLinhas(self, df, filtro):
        filtro_drop = df.loc[filtro]
        df.drop(filtro_drop.index, axis=0, inplace=True, errors='ignore')
        return df

    def alterarTipo(self, df, tipos):
        df = df.astype(tipos, errors='ignore')
        return df

    def validarColunas(self, df, lista):
        listaNf = []
        for item in lista:
            if item not in df.columns: listaNf.append(str(item))
        # for col in df.columns:
        #     if col not in lista: 
        #         listaNf.append(str(col))
        if len(listaNf) > 0: self.sair(listaNf)

    def gerarSaida(self, df):
        df.fillna(0, inplace=True)
        # df.replace("nan", 0)
        df = self.alterarTipo(df, str)

        df_reordena = ['TIPO ENTRADA', 'RANK_CLUSTER', 'CLUSTER', 'OBSERVAÇÃO', 'GH', 'RANK_FILIAL', 'FILIAL DESTINO',
                       'PRIORIDADE', 'MERCADORIA', 'DESCRICAO', 'QTDE', 'CUB', 'CUSTO', 'QTD_FILIAL', 'CUB_FILIAL',
                       'CUSTO_FILIAL',
                       'QTD_CLUSTER', 'CUB_CLUSTER', 'CUSTO_CLUSTER', 'VEICULO PLANO', 'VEICULO LOJA',
                       'FECHAMENTO 1200', 'DIA ENTREGA LOJA', 'DD ESCOAMENTO', 'FREQ', 'CUB SEMANA', 'ETG_TTL',
                       'CUB SEG', 'ETG_SEG', 'CUB TER', 'ETG_TER', 'CUB QUA', 'ETG_QUA', 'CUB QUI', 'ETG_QUI',
                       'CUB SEX', 'ETG_SEX',
                       'CUB SUPR', 'CUB PA', 'CUB TP', 'CLASSIFICACAO', 'SINALIZADOR',
                       'Aging DD', 'TIPO ITEM', 'SETOR', 'CHIP', 'SITUACAO', 'TIPO PEDIDO', 'PEDIDO DE VENDA', 'PEDIDO',
                       'DATA ENTRADA', 'DT CARGA PTO', 'CARGA PTO']

        """
        'FILIAL ENTREGA', 'ESTOQ.FIL',
        DDV_FUTURO', 'DDV_SO'
        'MUNICIPIO', 'UF',
        , 'TIPO DE CARGA', 'CARGA ENTREGA', 'BOX', 'DT.INCLUSAO CARGA.ETG', 'STATUS DA CARGA'
        ]
        """

        self.validarColunas(df, df_reordena)
        df = self.reordenarColunas(df, df_reordena)

        col_replace = ['QTDE', 'CUB', 'CUSTO',
                       'QTD_CLUSTER', 'CUB_CLUSTER', 'CUSTO_CLUSTER',
                       'QTD_FILIAL', 'CUB_FILIAL', 'CUSTO_FILIAL', 'CUB PA', 'CUB TP', 'CUB SUPR', 'DD ESCOAMENTO',
                       'GH', 'FREQ', 'ETG_SEG', 'ETG_TER', 'ETG_QUA', 'ETG_QUI', 'ETG_SEX', 'ETG_TTL', 'CUB SEMANA',
                       'CUB SEG', 'CUB TER', 'CUB QUA', 'CUB QUI', 'CUB SEX', 'RANK_CLUSTER', 'RANK_FILIAL']

        for col in df.columns:
            df[col] = df[col].str.strip()
            if col in col_replace:
                df[col] = df[col].str.replace('.', ',', regex=True)

        while True:
            try:
                df.to_csv(self.destino + 'Base_resultado.csv', index=False, sep=";", encoding='latin-1')
                break
            except Exception as e:
                mb.showerror('Favor, fechar base de resultado.', 'Confirmar para tentar novamente.')


if __name__ == '__main__':
    executa = Preencher_Carga()
    executa.start()
