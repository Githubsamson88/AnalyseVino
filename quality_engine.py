
import logging
import os
from glob import glob
import numpy as np
import pandas as pd
import codecs
import pickle
import shutil

#from safir_engine.hdf5_manager_quality import Hdf5ManagerQuality
#from safir_engine.hdf5_reader_quality import Hdf5ReaderQuality

class QualityEngine:
    logger = logging.getLogger('quality_engine')

    def __init__(self, mode="run", store_option="pickle"):
        """
        @brief: run of read the quality related data
        mode, str in ['read', 'run']
        """
        
        try:
            quality_file_path = os.environ['INPUT_QUALITY_FILE_PATH']
            if not os.path.isdir(quality_file_path):
                raise FileNotFoundError("Le chemin INPUT_QUALITY_FILE_PATH doit être un dossier")
            
        except KeyError:
            print("Error : la variable d'environnement INPUT_QUALITY_FILE_PATH n'a pas été setté. Elle doit indiquer le chemin du dossier des export SAP de la qualité matière")
            
        self.input_quality_file_path = quality_file_path
        
        try:
            self.output_quality_file_path = os.environ['OUTPUT_QUALITY_FILE_PATH']
        except KeyError:
            print("WARNING : la variable environnement OUTPUT_QUALITY_FILE_PATH n'a pas été setté, valeur defaut : './'")
            self.output_quality_file_path = './quality.' + store_option
            if os.path.exists(self.output_quality_file_path):
                print('WARNING : file {} already exists, will be deleted'.format(self.output_quality_file_path))
        if store_option == "hdf5":            
            if mode == "run":
                self.hdf5_manager = Hdf5ManagerQuality(self.output_quality_file_path)
            else:
                self.hdf5_manager = Hdf5ReaderQuality(self.output_quality_file_path)
        elif store_option == "pickle":
            print('store option is pandas dataframe pickle')
        
            if mode == "readonly":
                with open(self.output_quality_file_path, 'rb') as of:
                    self.df = pickle.load(of)

        
        
        self.store_option = store_option
            
        
    def saveRes(self):
        
        if os.path.isfile(self.output_quality_file_path):
            os.remove(self.output_quality_file_path)
        with open(self.output_quality_file_path, 'wb') as of:
            pickle.dump(self.df, of)
    
    def run(self):
        self.logger.info('Quality engine START')
        
        self.df_list = []
        file_list = glob(self.input_quality_file_path + '*.txt')
        file_list = sorted(file_list, key=lambda r: os.path.basename(r))
        for file in file_list:
            res_v = None
            labels = []
            print('file : ', file)
            df = pd.read_csv(file, sep='|', header=3, encoding="latin-1", engine="python", quoting=3)
            cols = df.columns
            cols = [c.strip() for c in cols]
            df.columns = cols
            for c in cols:
                #if df[c].dtype == str:
                df[c] = df[c].astype(str).str.strip()
            self.df_list.append(df)
        
        self.df = pd.concat(self.df_list)
        del self.df_list
        if self.store_option == "hdf5":
            self.hdf5_manager.addFromDataframe(self.df)
            self.hdf5_manager = Hdf5ReaderQuality(self.output_quality_file_path)
        elif self.store_option == "pickle":
            self.saveRes()
            
        self.logger.info('Quality engine STOP')

