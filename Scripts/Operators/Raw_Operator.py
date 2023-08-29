import sys
import yfinance
from pathlib import Path
from os.path import join
from airflow.models import BaseOperator
from pyspark.sql import SparkSession, functions as f
from datetime import datetime,timedelta
from time import sleep
class extracao_finance():
    def __init__(self,path,start_date,end_date,ticker = "AAPL",**kwargs):
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date
        self.path = path
        self.spark = SparkSession\
            .builder\
            .appName("extracao_Finance")\
            .getOrCreate()
        super().__init__(**kwargs)


    def criando_pasta(self):
        print("Criando pasta")
        subfolder_path = self.path
        (Path(subfolder_path).parent).mkdir(exist_ok=True, parents=True)
        (Path(subfolder_path)).mkdir(exist_ok=True)    

    def extraindo_dados(self): 
        sleep(2)
        try:    
            dados_hist = yfinance.Ticker(ticker="AAPL").history(
                interval="1d",
                start=self.start_date,
                end= self.end_date,         
                prepost=True
            )
            if not dados_hist.empty:
                dados_hist = dados_hist.reset_index()
                
                dados = self.spark.createDataFrame(dados_hist)

                dados = dados.drop('Stock Splits')

                dados = dados.withColumn("Open", f.round(f.col('Open'),2))
                dados = dados.withColumn("High", f.round(f.col('High'),2))
                dados = dados.withColumn("Low", f.round(f.col('Low'),2))
                dados = dados.withColumn("Close", f.round(f.col('Close'),2))
                dados = dados.withColumn("Date", f.split(f.col('Date'),' ')[0])
                dados = dados.dropDuplicates(subset=["Date", "High","Open"])
                self.dados = dados
            else: 
                print("No price data found for the given date range.")    
        except: 
            pass

    def execute (self):

        self.criando_pasta()
        try:
            self.extraindo_dados()
            if hasattr(self, 'dados') and not self.dados.isEmpty():
                self.dados.coalesce(1).write.mode('overwrite').csv(f'{self.path}', header=True)
            else: print("Nothing to Add")
        except:
            print("Nothing to Add")
        
        finally:
            self.spark.stop()    

if __name__ == '__main__':
    start_date= (datetime.now() - timedelta(days=240)).strftime('%Y-%m-%d')
    end_date= (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d')
    Base_folder = join(Path('~/Documents').expanduser(),
                    ('Sprinklr_Airflow/dadosvm/Airflow_Investimento/datalake/{stage}/Data={date}'))
    extracao = extracao_finance(path=Base_folder.format(stage= 'Raw',date=f'{start_date}'), start_date=start_date, end_date=end_date)
    extracao.execute()


