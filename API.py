from kaggle.api.kaggle_api_extended import KaggleApi
api = KaggleApi()
api.authenticate()

api.dataset_download_files('JosephW20/uk-met-office-weather-data')

zf = ZipFile('uk-met-office-weather-data.zip')

zf.extractall()
zf.close()