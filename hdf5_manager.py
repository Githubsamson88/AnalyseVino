

import h5py

import numpy
import pandas as pd
import os



class Hdf5Manager:
    
    
    def __init__(self, hdf5_file_path, deleteIfExists=True):
        
        self.offset = 0
        self.listcols = []
        mode = "w"
        # initialize file if file does not exist
        if os.path.exists(hdf5_file_path):
            print('file {} was already existant'.format(hdf5_file_path))
            mode = "a"
            if deleteIfExists:
                print('deleteIfExists=True : will delete the file')
                mode = "w"
            elif os.path.isdir(hdf5_file_path):
                raise ValueError('expected {} to be  a file, not a folder'.folder(hdf5_file_path))
            else:
                # check it is indeed an hdf5 file
                pass
        
        self.h5file = h5py.File(hdf5_file_path, mode)
        if 'data' not in self.h5file.keys():
            self.h5columns = self.h5file.create_group('data')
        
        else:
            self.h5columns = self.h5file['data']
    
    def addFromDataframe(self, df):
        """
        brief: 
        """
        if len(self.h5columns.keys()) == 0:
            self.initColumns(df.columns)
        
        for col in df.columns:
            if col not in self.h5columns.keys():
                print("------------------------------------------------")
                print('col : ', col)
                self.initColumns([col])
                #self.h5columns[c].resize(self.offset, axis=0)
                hc = self.h5columns[col]
                hc.resize(self.offset, axis=0)
                hc[0:] = numpy.full(self.offset, numpy.nan)

            col_dataset = self.h5columns[col]
            sz0 = col_dataset.shape[0]
            col_dataset.resize(sz0 + len(df), axis=0)
            if col == 'TIME':
                col_dataset[sz0:] = df[col].to_numpy().view('<i8')
            else:
                col_dataset[sz0:] = df[col].to_numpy()
                
        col_not_updated = [c for c in self.listcols if c not in df.columns]
        for c in col_not_updated:
            print('col left : ', c)
            hc = self.h5columns[c]
            sz = hc.shape[0]
            hc.resize(sz + len(df), axis=0)
            hc[sz:] = numpy.full(len(df), numpy.nan)
        #cols_left = [c for c in self.listcols if c not in col]

        
        self.offset = self.offset + len(df)
        print('self.offset is : ', self.offset)
        
    
    
    def initColumns(self, cols):
        """
        brief : init datagroup's datasets
        cols, list[str], name of the columns,
                all columns except TIME are in float32
        """
        
        self.listcols += list(cols)
        for col in cols:
            if col == 'TIME':
                dtp = '<i8'
            else:
                dtp = 'float16'
            self.h5columns.create_dataset(col, (0,), dtype=dtp, maxshape=(None,))

        return

    
            
    def __del__(self):
        if hasattr(self, 'h5file'):
            self.h5file.close()
        
            
            
                
                
