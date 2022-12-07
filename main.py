from lib2to3.pytree import convert
from operator import index
from textwrap import indent
import time
import requests
import pandas
import json
import csv
import pyodbc
from django.shortcuts import render,HttpResponse

from pyjstat import pyjstat
from matplotlib import pyplot


#Link onde iremos buscar as API do BPSTATs
BPSTAT_API_URL = "https://bpstat.bportugal.pt/data/v1"
LANGUAGE = "PT"


#Série da API a consultar - https://bpstat.bportugal.pt/serie/12561507
series_id = "12561507"

#Os dados estão armazenados num conjunto, então temos de descobrir que conjunto é esse
print("A procurar os dados da série...")
series_url = f"{BPSTAT_API_URL}/series/?lang={LANGUAGE}&series_ids={series_id}"
series_metadata = requests.get(series_url).json()[0]

domain_id = series_metadata["domain_ids"][0] #Como é apenas uma série, basta selecionarmos o primeiro domínio estatístico dessa série
dataset_id = series_metadata["dataset_id"]  #O dataset é onde estão os dados atuais da série

#Vai procurar os valores da série ao longo do tempo, em formado JSON-stat
print("A procurar as observações da série...")
dataset_url = f"{BPSTAT_API_URL}/domains/{domain_id}/datasets/{dataset_id}/?lang={LANGUAGE}&series_ids={series_id}"
dataset_data = pyjstat.Dataset.read(dataset_url)

print("A processar os dados...") 

#Converter para um dataframe
dataframe = dataset_data.write("dataframe")

#O JSON-stat consegue encontrar a dimensão do tempo, pois há apenas um       
time_dimension_id = dataset_data["role"]["time"][0] 
time_label = dataset_data["dimension"][time_dimension_id]["label"]

#Converte o dataframe para um data adequado com o pandas
dataframe[time_label] = pandas.to_datetime(dataframe[time_label])

#Seleciona os dados a converter
columns = dataframe[[time_label, "value"]]

#Converte o dataframe numa tabela csv
#convert=pandas.DataFrame(dataframe)
#convert.to_csv('DividaPublica.csv')
#print(teste2)

dados = pandas.read_csv(r'DividaPublica.csv') #Lê o ficheiro csv

servidor = 'ANDRECAMPOS\SQLEXPRESS'
banco_dados = 'BancoDePortugal'
utilizador = ''
senha = ''

conectar = pyodbc.connect('DRIVER={SQL Server};SERVER='+servidor+';DATABASE='+banco_dados+';UID='+utilizador+';PWD='+senha)
cursor = conectar.cursor()

for indice, cada_linha in dados.iterrows():
    cursor.execute('insert into DividaPublica (Indice, Consolidacao, DivisaDeReferencia, Fonte, FluxosPosicaoePrecos, InstrumentoFinanceiro, MetodoDeValorizacao, Metrica, Otica, Periodicidade, PrazoResidual, PrazoOriginal, SetorInstitucionalDeContraparte, SetorInstitucionalDeReferencia, TerritorioDeContraparte, TerritorioDeReferencia, TipoDeInformacao, UnidadeDeMedida, Data, value) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', cada_linha.Indice, cada_linha.Consolidacao, cada_linha.DivisaDeReferencia, cada_linha.Fonte, cada_linha.FluxosPosicaoePrecos, cada_linha.InstrumentoFinanceiro, cada_linha.MetodoDeValorizacao, cada_linha.Metrica, cada_linha.Otica, cada_linha.Periodicidade, cada_linha.PrazoResidual, cada_linha.PrazoOriginal, cada_linha.SetorInstitucionalDeContraparte, cada_linha.SetorInstitucionalDeReferencia, cada_linha.TerritorioDeContraparte, cada_linha.TerritorioDeReferencia, cada_linha.TipoDeInformacao, cada_linha.UnidadeDeMedida, cada_linha.Data, cada_linha.value)
conectar.commit()
cursor.close()