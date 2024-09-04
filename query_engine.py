
import os
import json
import numpy as np
import dask.bag as db
from datetime import datetime
import pickle
from pickle_reader import PickleReader
from events_engine import EventsEngine
import re

class QueryEngine:
    
    def __init__(self, storage_option="jsonfile"):
        
        
        if storage_option != "jsonfile":
            raise ValueError('seul storage_option=jsonfile est supporté pour le moteur de requete')
        
        self.sensor_reader = PickleReader()
        try:
            self.json_file_path = os.environ['JSON_FILE_PATH']
            if not self.json_file_path.endswith('/'):
                self.json_file_path = self.json_file_path + '/'
        except KeyError:
            print("WARNING : la variable environnement JSON_FILE_PATH n'a pas été setté, valeur defaut : './'")
            self.json_file_path = './'
            
        self.recettes = self.getDb(self.json_file_path + 'RECETTES.json')
        self.etapes = self.getDb(self.json_file_path + 'ETAPES.json')
        self.sequences = self.getDb(self.json_file_path + 'SEQUENCES.json')
        self.operations = self.getDb(self.json_file_path + 'OPERATIONS.json')
        self.fonctions = self.getDb(self.json_file_path + 'FONCTIONS.json')
        self.capteurs = self.getDb(self.json_file_path + 'CAPTEURS.json')
        self.operateurs = self.getDb(self.json_file_path + 'OPERATEURS.json')
        

        
        if os.path.exists(self.json_file_path + 'MODIF_DICT.pickle'):
            with open(self.json_file_path + 'GLOBAL_DICTS.pickle', 'rb') as of:
                self.global_dict = pickle.load(of)
            with open(self.json_file_path + 'LIST_ID.pickle', 'rb') as of:
                self.list_id = pickle.load(of)
            with open(self.json_file_path + 'LIST_MODIF.pickle', 'rb') as of:
                self.list_modif = pickle.load(of)
            with open(self.json_file_path + 'MODIF_DICT.pickle', 'rb') as of:
                self.modif_dict = pickle.load(of)
            
        else:
            self.convertDbsToDicts()
            with open(self.json_file_path + 'GLOBAL_DICTS.pickle', 'wb') as of:
                pickle.dump(self.global_dict, of)
            with open(self.json_file_path + 'LIST_ID.pickle', 'wb') as of:
                pickle.dump(self.list_id, of)
            with open(self.json_file_path + 'LIST_MODIF.pickle', 'wb') as of:
                pickle.dump(self.list_modif, of)
            with open(self.json_file_path + 'MODIF_DICT.pickle', 'wb') as of:
                pickle.dump(self.modif_dict, of)
                
            if not os.path.exists(self.json_file_path + 'GLOBAL_DICTS.pickle'):
                raise FileNotFoundError(self.json_file_path + 'GLOBAL_DICTS.pickle')
        
    def convertDbsToDicts(self):
        """
        brief 
        """
        global_dict = {}
        list_id = {'etapes': [], 'sequences': [], 'operations': [], 'fonctions': []}
        list_modif = {'etapes': [], 'sequences': [], 'operations': [], 'fonctions': []}
        modif_dict = {}
        
    
        for i, base in enumerate(['ETAPES', 'SEQUENCES', 'OPERATIONS', 'FONCTIONS']):
           
            print('converting ', base)
            baselower = base.lower()        
            with open(self.json_file_path + base + '.json', 'rb') as of:
                bj = json.load(of)
                for v in bj:
                    if base != 'ETAPES':
                        reg = re.search('D[.1-9]* (.*)', v['modifications'])
                        if reg:
                            v['modifications'] = reg.group(1)
                        modif_dict[v['modifications']] = [v]
                        list_modif[baselower].append(v['modifications'])
                            

                    global_dict[v['id']] = v
                    list_id[baselower].append(v['id'])
                
                    
            
        self.global_dict = global_dict
        self.list_id = list_id
        self.list_modif = list_modif
        self.modif_dict = modif_dict

        for u in ['etapes', 'sequences', 'operations', 'fonctions']:
            self.list_id[u] = sorted(list_id[u])
            self.list_modif[u] = sorted(list_modif[u])
    
    def getJson(self, fpath):
        res = None
        with open(fpath, 'r') as fo:
            res = json.load(fo)
        
        if len(res) != 1:
            raise ValueError()
        
        return res['data']
    
    def getDb(self, fpath):
        b = db.read_text([fpath]).map(json.loads)
        b = b.flatten()
        return b
    
    def get_unite_type(self, capteur):
        """
        brief 
        """
        return self.capteurs.filter(lambda r: r['nom'] == capteur).map(lambda r: [r['unite'], r['type']]).flatten().compute()
    
    def get_ids(self, coll='fonctions'):
        
        #res = getattr(self, coll).map(lambda r: r['id']).compute()
        #return sorted(res)
        return self.list_id[coll]
        
    
    def get_operateurs(self):
        return self.operateurs.filter_map(mapfunc=lambda r: r['creation'], compute=True, flatten=False)
    
    def get_operateur_of_etape(self, etape, fullname=False):

        id_field = 'creation'
        if fullname:
            id_field = 'user'
            
        return self.filter_map('operateurs', filterfunc=lambda r: r['lib']==etape, mapfunc=lambda r: r[id_field], flatten=False)
    
    def get_operateur_info_of_etape(self, etape):
        return self.filter_map('operateurs', filterfunc=lambda r: r['lib']==etape, compute=True, flatten=False)


    def get_time_etape(self, etape):
        res = self.get_operateur_info_of_etape(etape)

        if len(res) > 1:
            print("WARNING : l'etape {} est {} fois mentionnée, la dernière sera prise".format(etape, len(res)))
        e = res[-1]
            
        d0 = datetime.strptime(e['date0'], '%d/%m/%Y %H:%M:%S')
        d1 = datetime.strptime(e['date1'], '%d/%m/%Y %H:%M:%S')
        
        return [d0, d1]
    
    def get_modifications(self, coll):
        #return getattr(self, coll).map(lambda r: r['modifications']).distinct().compute()
        
        return self.list_modif[coll]
    
    def filter_map(self, coll, filterfunc=None, mapfunc=None, distinct=False, compute=True, flatten=True):
        """
        brief : general query function, 
        coll, str, base to query
        filterfunc, function or None, used for filtering
        mapfunc, function or None, used for mapping
        distinct, boolean, whether to apply distinct method
        compute, boolean, whether to compute, or simply return the object
        flatten, boolean, whether to flatten the result
        """
        
        res = getattr(self, coll)
        if filterfunc is not None:
            res = res.filter(filterfunc)
        if mapfunc is not None:
            res = res.map(mapfunc)
        if distinct:
            res = res.distinct()
        
        if flatten:
            res = res.flatten()

        if compute:
            return res.compute()
        else:
            return res
    
    def get_by_modifications_rapid(self, modifications, exactmatch=True):
        
        
        if exactmatch:
            return self.modif_dict.get(modifications, [])
        else:
            list_modifs = []
            for base in ['sequences', 'operations', 'fonctions']:
                aux_l = [m for m in self.list_modif[base] if m.endswith(modifications)]
                list_modifs += aux_l
            list_modifs = list(set(list_modifs))
            res = []
            for e in list_modifs:
                res += self.modif_dict[e]
            return res
     
                
    
    def get_by_modifications_tout_niveaux(self, modifications, mapfunc=None, etape=None, exactmatch=False):

        res  = []
        for coll in ['sequences', 'operations']:#, 'fonctions']:
            filterfunc = None
            # if etape is not None:
            #     filterfunc = lambda r: r['id'].split('-')[1].startswith(etape)# or r['id'].split('_')[1].startswith(etape)
            res += self.get_by_modifications(coll, modifications, mapfunc=mapfunc, exactmatch=exactmatch,
                                             etape=etape)
        
        res = sorted(res, key=lambda r:  r['temps_executer']['$date'] if ('temps_executer' in r) else 0)
        return res
            
    def get_by_modifications(self, coll, modification, mapfunc=None, exactmatch=False, filterfunc=None, etape=None):
        
        print('etape is : ', etape)
        #filters = []
        if not exactmatch:
            #filters += [lambda r: modification in r['modifications'].strip()]
            res = getattr(self, coll).filter(lambda r: modification in r['modifications'].strip())
        else:
            #filters += [lambda r: r['modifications'].strip().endswith(modification)]
            res = getattr(self, coll).filter(lambda r: r['modifications'][3:].strip().endswith(modification))
        

        if etape is not None:
            res = res.filter(lambda r: r['etape_associee'] == etape)
        
        if mapfunc is not None:
            res = res.map(mapfunc)
        res = res.compute()
        
        res = sorted(res, key=lambda r:  r['temps_executer']['$date'] if ('temps_executer' in r) else 0)
        return res

    
    def list_sensors(self, field='nom'):
        res = self.filter_map('capteurs', filterfunc=lambda r: True, mapfunc=lambda r: r['nom'], flatten=False)
        return res
    
    def display_results(self, modifications, type_element='sequences'):
        pass
    
    def get_sequences_by_etape_id(self, etape_id):
        res = [self.global_dict[k] for k in self.global_dict.keys() if k.startswith(etape_id) and k in self.list_id['sequences']] 
        return res
    
    def get_operations_by_parent_id(self, parent_id):
        """
        brief query arg field of operations composing the parent with id parent_id
        @param :
        parent_id, str, id of parent
        parent, str in ["sequence", "etape_lot", "lot"]
        arg, str, field to keep from the operation objects in results, e.g. : modifications, id ..
        """
        res = [self.global_dict[k] for k in self.global_dict.keys() if k.startswith(parent_id) and k in self.list_id['operations']]
        return res
    
      
    def get_elements_by_parent_id(self, parent_id, element_type=None, arg=None):
        """
        brief query elements of a collection composing the parent with id parent_id
        @param:
        parent_id, str, id of parent
        element_type, str in : ['fonctions', 'operations', 'sequences', 'etapes']
        arg, str or None, field to keep from the fonction objects in result, e.g. : modifications, id ..,
                          if None, will return all fields as dict
        """
        
        if element_type not in ['fonctions', 'etapes', 'operations', 'sequences'] :
            raise ValueError("element_type doit être renseigne. choix possible : 'etapes', 'sequences', 'operations', 'fonctions'")
        
        res = [self.global_dict[k] for k in self.global_dict.keys() if k.startswith(parent_id) and k in self.list_id[element_type]]
        return res
    
    def get_fonctions_by_parent_id(self, parent_id):
        """
        """
        vals = [k for k in self.list_id['fonctions'] if k.startswith(parent_id)]
        res = [self.global_dict[v] for v in vals]
        return res

    def get_element_by_id(self, id_val):
        """
        brief query element by its id
        id_val, str, id of element
        coll, str, collection where the element is
        """
        
        res = self.global_dict.get(id_val, None)
        if res is None:
           print('{} not found'.format(id_val))
        return res 
       
    def get_measures_interval(self, date0, date1, sensors=None):
        return self.sensor_reader.timeQuery(date0, date1, cols=sensors, timeType="datetime")
        
    def get_measures_for_element(self, id_val, sensors=None, coll='fonctions'):
        """
        brief retrieve sensor measures for a given element (of type fonction, operation, sequence)
        id_val, str, id of the element
        sensors, list[str], sensors concerned for retrieval
        coll, str in : ['fonctions', 'operations', 'sequences']
        """
       
        if coll not in ['fonctions', 'operations', 'sequences', None]:
            raise ValueError('collection {} not in {}'.format(coll, ['fonctions', 'operations', 'sequences']))
        
        
        elm = self.global_dict.get(id_val, None)#self.get_element_by_id(id_val, coll=None)
        if elm is None:
            return None
        
        
        if 'temps_executer' in elm and 'temps_terminer' in elm:
            return ['complete', self.sensor_reader.timeQuery(elm['temps_executer']['$date'], elm['temps_terminer']['$date'], cols=sensors, timeType="timestamps")]
        elif 'temps_terminer' in elm and coll=="fonctions":# in the case of functions, it is the case that the action is discrete (no interval)
            return ['no-start', None]
        elif 'temps_terminer' in elm and coll!='fonctions':
            return ['no-start', self.sensor_reader.timeQuery(np.NaN, elm['temps_terminer']['$date'], cols=sensors, timeType="timestamps")]
        elif 'temps_executer' in elm:
            return ['no-end', self.sensor_reader.timeQuery(elm['temps_executer']['$date'], np.NaN, cols=sensors, timeType="timestamps")]
        else:
            raise ValueError('no time start or time end for chosen element')
        
        # plt.plot(range(0,len(res), 1),res['BC230ZP01'].values)
    
    @staticmethod
    def convertToDate(int_vals):
        return [datetime.fromtimestamp(int_val / MULT) for int_val in int_vals]
    
    
    def filter_relevant_sensor(measures):
        """
        brief, try to filter the relevant sensors for the interval : first approach is keep the ones with variation
        """
        pass
    
    
