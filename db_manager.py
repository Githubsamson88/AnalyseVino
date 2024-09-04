
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import types
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy.sql.expression import any_, or_, join, between, and_
"""
join (left, right, onclause=None, isouter=False, full=False)
j = join(user_table, address_table, user_table.c.id == address_table.c.user_id)

select([user_table]).select_from(join(j))
"""

"""
from sqlalchemy import between
stmt = select([users_table]).where(between(users_table.c.id, 5, 7))
"""


"""
different 
from sqlalchemy import join
j = user_table.join(address_table,
                    user_table.c.id == address_table.c.user_id)
stmt = select([user_table]).select_from(j)
equivalent to 
    Select user.id, user.name from user
    Join address On user.id = address.user_id
"""


from sqlalchemy import CheckConstraint

from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import Table, Column, Integer, String, Float, MetaData, ForeignKey, DateTime, Interval
from sqlalchemy.sql import select

from sqlalchemy.ext.declarative import declarative_base

#from tableschema import Table


TIME_COL = 'TIME'
CHUNK_SIZE = 10000

class DBManager:
    
    def __init__(self, db_url=None, db_type=None, client=None, user=None, password=None, ip=None, port=None, db=None):

        if db_url is None:
            engineStr = self.create_db_uri(db_type, client, user, password, ip, port, db)
            self.engineStr = engineStr
        else:
            self.engineStr = db_url
            
        self.metadata = MetaData()
        engine = create_engine(self.engineStr)
        if not database_exists(engine.url):
            create_database(engine.url)
        self.engine = engine
        
        # inspect db from metadata object
        self.metadata.reflect(self.engine)
        
        session = sessionmaker(bind=engine)
        self.session = session()
        
        connection = self.engine.connect()
        self.conn = connection
        
        # set to True when fast time query is ready
        self.hasResall = False

    @staticmethod
    def create_db_uri(db_type, client, user, password, ip, port, db):
        userLogin = "" if user is None else user
        userLogin = userLogin +'@' if password==None else userLogin + ':' + password+'@'
        address = ip if port is None else ip + ':' + port
        engineStr = db_type + '+' + client + '://' + userLogin + address + '/' + db
        return engineStr
    
    @property
    def tables(self):
        return dict(self.metadata.tables)

    
    def getPrimaryKey(self, tableStr):
        """
        brief, get primary key
        tableStr, str, table name
        returns, str, primary key field or False
        """
        
        if tableStr not in self.tables:
            raise ValueError('table {} was not found'.format(tableStr))
        
        lPrimary = self.tables[tableStr].primary_key.columns
        lenPrim = len(lPrimary) 
        if lenPrim == 1:
            return lPrimary.keys()[0] # items()
        elif lenPrim == 0:
            return False
        else:
            raise ValueError("expected list of primary keys to be of length 1, got {}".format(len(lPrimary)))
        
        
    def setPrimaryKey(self, tableStr, key=TIME_COL, createKey=False):
        """
        brief : set the primary key
        tableStr, str, name of the table
        key, str, field to be set as the primary key, default is "TIME"
        raise ValueError : in case the table already has a primary_key
        """
        
        if tableStr not in self.tables:
            raise ValueError('not table name {} found'.format(tableStr))
        
        hasGotPrimary = self.getPrimaryKey(tableStr)
        if hasGotPrimary:
            raise ValueError('Cannot set primary key on table {}. Already has primary key : {}'.format(tableStr, hasGotPrimary))
            
        if key in self.tables[tableStr].c:
            inst = text('alter table {} add primary key({})'.format(tableStr, key))
        elif createKey:
            inst = text('alter table {} add {} serial primary key'.format(tableStr, key))
        else:
            raise ValueError('field {} was not found in table {}'.format(key, tableStr))
        
        self.conn.execute(inst)
        self.session.flush()
        self.session.close()
        self.metadata.create_all(self.engine)
    
    def setIndex(self, tableName="sensors", col="TIME"):
        """
        brief : set index to table
        """
        sql = text("""
                   CREATE UNIQUE INDEX index_name ON {table_name} ( {col});
        ;""".format(table_name=tableName, col=col))
        self.conn.execute(sql)#session.execute(sql)
        self.session.flush()
        self.session.close()
    
    def createTable(self, tableName, columns=[], tableConstraints=[]):# , force=True):
        """
        brief : create table in the db in use in this DBM instance (default db_s)
        tableName: String, name of the table to create
        columns: list of tuples of (String, Type, dict) for defining a column
            String : name of column, e.g. 'id'
            Type : type of column, e.g. Integer, String(20), String(30)
            args : list of additional args, e.g. CheckConstraint(..)
            dict : options, e.g. {'primary_key':True, 'index': True}        
        raises FileExistsError, when the table name given is already in db
        """
        
        if tableName in self.metadata.tables.keys():
            raise FileExistsError(tableName)
        
        # Base = declarative_base()
        cols = []
        for c in columns:
            colname = c[0]
            colType = c[1]
            opts = {}
            args = []
            if len(c) > 2:
                args = c[2]
                args = [eval(a) for a in args]
            if len(c)> 3:
                opts = c[3]
            if isinstance(colType, str):
                colType = eval(colType)
            #print('colname: ', colname)
            #print('colType : ', colType)
            #print('*args')
            #print('calling Column constructor with : colname : ', colname, ' colType : ', colType, ' args: ', args, ' opts : ', opts)
            cols.append(Column(colname, colType, *args, **opts))
        cols += [eval(t) for t in tableConstraints]
        #print('cols before calling Table constructor : ', cols)
            
        table = Table(tableName, self.metadata, *cols)
        
        self.metadata.tables[tableName] = table
        self.metadata.create_all(self.engine)
    
    
    def insertValueInTable(self, tableStr, **kwargs):
        """
        brief : insert value into a table
        tableStr, String, table name
        kwargs, dict, keyvaue pair to add to table
        """
        if tableStr not in self.tables:
            raise FileNotFoundError(tableStr)
        tble = self.tables[tableStr]
        ins = tble.insert().values(**kwargs)
        self.conn.execute(ins)
        self.session.flush()
        self.session.close()
        
    
    def deleteTable(self,tablename):
        """
        brief : delete a table from db
        tablename : String, name of the table to delete
        """
        if tablename in self.tables:
            del self.tables[tablename]
        sql = text('DROP TABLE IF EXISTS {};'.format(tablename))
        result = self.engine.execute(sql)
        self.session.flush()

    
    def listTables(self, detail=True):
        for (tname, t) in self.tables.items():
            if detail:
                print('-'*100)
            print('TABLE ', tname)
            if detail:
                for c in t.columns:
                    if c.primary_key:
                        primaryKey = " primary_key : True"
                    else:
                        primaryKey = ""
                    print('column: {:<20} type: {:<30} {:<30}'.format(c.name, str(c.type), str(primaryKey))) # c.description 
            

    def timeQuery(self, startDate, endDate, tableStr="sensors", columns=None, limit=None):
        """
        query using start and end datetime
        startDate, String of the form yyyy-mm-dd --:--:--, starting datetime
        endDate, String, of the form yyyy-mm-dd --:--:--, ending datetime
        tableStr, String, table name
        columns, list[String], restrict result to these columns, if None, then keep all columns
                            the time columns is handled separately
        limit, int or None, if not None, limit to number of rows to return

        Example: 
           timeQuery('2019-09-01 00:03:30', '2019-09-01 01:00:00', columns=['BC211ZP01', 'BC210ZP01'])
        """

        # prepare list of smt columns
        smt = self.tables[tableStr]
        if columns is None:
            smtlist = [smt]
        else:
            if 'TIME'  in columns:
                columns.remove('TIME')
            columns.insert(0, 'TIME')
            smtlist = [getattr(smt.c, col) for col in columns]
        if limit is not None:
            res = self.session.query(*smtlist).filter(and_(smt.c.TIME < endDate, smt.c.TIME > startDate)).limit(limit)
        else:
            res = self.session.query(*smtlist).filter(and_(smt.c.TIME < endDate, smt.c.TIME > startDate))
            
        #res = self.session.query(self.tables[tableStr])
        self.session.close()
        return res#.all()#self.convertResToArray(res)
    
    def initFastTimeQuery(self, tableStr="sensors", columns=None, limit=None):
        """
        query all the table
        """
        print('starting init for fast time queries')
        smt = self.tables[tableStr]
        smtlist = [smt]
        res = self.session.query(*smtlist)
        print('running res.all()')
        #resall = res.all()
        cols = list(self.tables[tableStr].columns.keys())
        print("running dataframe.from_records")
        resall = pd.DataFrame(res, columns=cols)# .from_records.( , coerce_float=True) #
        self.resall = resall
        self.hasResall = True
        print('init for fast time queries completed')

    def fastTimeQuery(self, startDate, endDate, tableStr="sensors", columns=None, to=None, filePath=None):
        """
        brief : rapid table query using in memory dataframe
        startDate, endDate: python datetime.datetime, start and end time
        tableStr, str, table to query, sensrs is default
        columns, None or list of str, columns to retrieve,
        to, None or "json" or "csv", format of output
        file_path, None or str, is to is not None, path for the output
        """
        if not self.hasResall:
            print('the db will be put in python object for speed issues')
            self.initFastTimeQuery()
        if not isinstance(startDate, datetime):
            if isinstance(startDate, str):
                startDate = datetime.strptime(startDate,"%Y-%m-%d %H:%M:%S")
            else:
                raise TypeError(startDate)
        if not isinstance(startDate, datetime):
            if isinstance(endDate, str):
                endDate = datetime.strptime(startDate,"%Y-%m-%d %H:%M:%S")
            else:
                raise TypeError(endDate)
                
        if columns is not None:
            res = self.resall[(self.resall['TIME'] >= startDate) & (self.resall['TIME'] <= endDate)][columns]
        else:
            res = self.resall[(self.resall['TIME'] >= startDate) & (self.resall['TIME'] <= endDate)]
        if to is not None:
            if to=="json":
                if filePath is None:
                    return res.to_json()
                else:
                    res.to_json(path_or_buf=filePath)
                    return
            elif to=="csv":
                if filePath is None:
                    return res.to_csv()
                else:
                    return res.to_csv(path_or_buf=filePath)
        else:
            return res
            
    def save_res(self, res, filepath, fmt="%.2f", delimiter="\t"):
        """
        save result from timeQuery to a csv format
        """
        firstLine = delimiter.join(res[0])
        formatCsv =  ["%s"]
        formatCsv += [fmt]*(len(res[1][0])-1)#['{%Y-%m-%d %H:%M:%S}']
        #formatCsv += ['%s']*(len(res[1][0])-1)#*(len(res[1][0])-2) + '%.3d'
        np.savetxt(filepath, res[1], delimiter=delimiter, fmt=formatCsv, header=firstLine, comments="")
    
    def convertResToArray(self, res):
        cols = [u['name'] for u in res.column_descriptions]
        #cols.remove('TIME')
        
        #strDtype = 'M8[s], ' + 
        strDtype = 'M8[s], ' + 'f4, '*(len(cols)-2) + 'f4'
        dt = np.dtype(strDtype)
        rn = np.empty([res.count(), 1], dtype=dt)
        #rn = np.empty([res.count(), len(cols)])
        rn = np.array(res.all(), dtype=dt)
        #rtime = np.emp
        #for i, v in enumerate(res):
        #    rn[i] = v
        return [cols, rn]
    
    
    def insertFileIntoTable(self, filepathList, tableStr, lineSeparator):
        """
        insert file into table, with custom processing
        """
        pass
    
    
    def emptyTable(self, tableStr):
        """
        empty table
        tableStr : String, name of table
        """
        if tableStr not in self.tables:
            raise FileNotFoundError(tableStr)
        sql = text('delete from {} where true ;'.format(tableStr))
        print('inside emptyTable method, sql query is : ')
        print(sql)
        self.conn.execute(sql)
    
    
    def evalQueryStr(self ):
        pass
    
    
    def selectQuery(self, tableConditions):
        """
        brief : run a select query
        tableConditions, a list of conditions
                    each condition is a dictionary of one of three forms : 
                    
                            key : operation 
                            value : list of conditions
                        or 
                            key : column object, e.g. user.c.id or "user.c.name"
                            value : condition of the form : " == 1", or "> 10"
                        or
                            key : binary expression (e.g. ==)
                            value : list of column objects, e.g. [user.c.tel, address.c.tel]
                        
        dictionary, keys are table names, e.g. "users"
                                     values are dictionaries :
                                                keys are column names, e.g. "id"
                                                values are the condition in the form : " > 
        """
        
        d = {'and' : {'action0022.c.temperature' : '> 20', 'action002.c.action' : '== "open"'}}
        
        # if len(listTableStr > 1) and len(kwargs) == 0:
        #     raise ValueError("""{} and no condition will
        #                      do a confusing join. Make sure you add
        #                      condition on the columns""".format("["+ ", ".join(listTableStr) + ']'))
        # if not kwargs:
        #     s = select(self.tables[tableStr])
        # else:
        #     # create where conditions
        #     for col in columns:
        #         pass
        

    def importFromDataframe(self, dt, tableName, ifExists='replace', chunksize=CHUNK_SIZE):
        """
        brief : import a pandas dataframe to the table
        dt, pandas.Dataframe, dt to be imported
        tableName, str, table where to import
        append, 'fail' : Raise ValueError if table exists,
                'replace' : Drop the table before inserting,
                'append' : insert new values to th existing table
        """
        
        if ifExists=="fail" and tableName in self.tables:
            raise ValueError(tableName)
        elif ifExists=='replace':
            if tableName in self.tables:
                self.deleteTable(tableName)
        elif ifExists!='append':
            raise ValueError(ifExists)
        
        cols = dt.columns
        
        dtpe = {c : types.Numeric(precision=4) for c in cols}
        dtpe.update({'TIME': types.DATETIME()})
        print('len of dtpe is ', len(dtpe))
        print('len of dt.columns', len(dt.columns))
        
        #strDtype = 'M8, ' + 'f8, '*(len(cols)-2) + 'f8'
        #dtpe = np.dtype(strDtype)
        dt.to_sql(tableName, self.engine, index=False, dtype=dtpe, if_exists=ifExists, chunksize=chunksize)#index=True, index_label="TIME"
        
        self.session.flush()
        self.session.close()
        self.metadata.create_all(self.engine)
        self.metadata.reflect(self.engine)
        #self.setIndex(tableName=tableName)
    
    #def groubByInterval
        
    # def __del__(self):
    #     if hasattr(self, 'engine'):
    #         self.engine.dispose()

def main():
    dbm = DBManager()
    dbm.createTable('hjk', [('col11', Integer, {'primary_key':True}), ('col33', String(10))])
    
if __name__ == "__main__":
    # testing the class
    pass
    dbm = DBManager()
    dbm.setEngine()
    
    #dbm.deleteTable('table003')
    #dbm.createTable('table003', columns=[('id1', 'Integer', [],  {'primary_key': True}), ('colab', 'String(30)')], tableConstraints=["CheckConstraint('char_length(some_string) > 2')"])

    #dbm.createTable('somenewfashion', columns=[('somecolA', 'Integer', [], {'primary_key': True}), ('somcolB', 'String(10)'), ('some_other', 'Integer')])


    smt = dbm.tables['sometble']
    res = dbm.session.query(smt).filter(smt.c.cola > 80)
    dbm.session.close()

