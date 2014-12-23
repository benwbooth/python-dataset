import io, sys
import sqlalchemy, pandas, pandas.io.sql, pandas.core.format, pandas.core.common, inflect, tabulate

class CopyCsv(object):
    def __init__(self, df, **kwargs):
        self.buffer = io.BytesIO() if sys.version_info[0] < 3 else io.StringIO()
        self.csv = pandas.core.format.CSVFormatter(df, self.buffer, **kwargs)
        self.csv.writer = pandas.core.common.UnicodeWriter(self.buffer, encoding='utf8')
        self.nrows = len(self.csv.data_index)
        self.chunks = int(self.nrows / self.csv.chunksize) + 1
        self.i = 0
        
    def read(self, size=8192):
        start_i = self.i * self.csv.chunksize
        end_i = min((self.i + 1) * self.csv.chunksize, self.nrows)
        if start_i >= end_i: return ''
        if self.i >= self.chunks: return ''
        self.csv._save_chunk(start_i, end_i)

        self.i += 1
        s = self.buffer.getvalue()
        self.buffer.truncate(0)
        self.buffer.seek(0)
        return unicode(s) if sys.version_info[0] < 3 else s

def pg_query(engine, sql, *args, **kwargs):
    index = kwargs.pop('index', False)
    index_col = kwargs.pop('index_col', None)
    coerce_float = kwargs.pop('coerce_float', True)
    parse_dates = kwargs.pop('parse_dates', None)
    params = kwargs.pop('params', None)
    chunksize = kwargs.pop('chunksize', None)
    schema = kwargs.pop('schema', 'pg_temp')
    if_exists = kwargs.pop('if_exists', 'fail')
    keep = kwargs.pop('keep', False)
    create = kwargs.pop('create', True)
    dfs = kwargs
    # set up variables
    connection = engine.connect()
    cursor = connection.connection.cursor()
    db = pandas.io.sql.SQLDatabase(connection)
    preparer = sqlalchemy.sql.compiler.IdentifierPreparer(engine.dialect)
    args = list(args)
    # process extra arguments: dicts are data frames, lists/tuples are params
    for arg in args:
        if isinstance(arg, dict):
            dfs.update(arg)
        elif isinstance(arg, list) or isinstance(arg, tuple):
            params += list(arg)
        elif isinstance(arg, pandas.DataFrame):
            if getattr(arg, 'name') is not None:
                dfs[str(getattr(arg, 'name'))] = arg
            else:
                raise Exception("Dataframe given with no name: {}".format(arg))
        else:
            raise Exception("Invalid argument: {}".format(arg))
    # remove temp files before importing the temp tables to avoid name clashes
    if len(dfs)>0 and schema=='pg_temp': connection.execute("discard temp")
    # load the data frames into temp tables
    try:
        for name, df in dfs.items():
            # create the table if it's a temp table
            table = pandas.io.sql.SQLTable(name, engine, frame=df, schema=schema, if_exists=if_exists, index=index)
            if schema == 'pg_temp': connection.execute(table.sql_schema())
            else:
                if not create and not engine.has_table(name, schema):
                    raise Exception("Table {} does not exist!".format(name if schema is None else '.'.join([schema,name])))
                table.create()
            # load the data into the table
            copycsv = CopyCsv(df, index=index)
            cursor.copy_expert('copy {} {} from stdout csv'.format(
                preparer.format_table(table),
                '('+','.join([preparer.format_column(c) for c in table.table.columns])+')' if len(table.table.columns)>0 else ''), copycsv)
        # run the query and return the results as a data frame
        return db.read_query(sql, index_col=index_col, coerce_float=coerce_float, parse_dates=parse_dates, params=params, chunksize=chunksize) if sql is not None else None
    finally:
        if not keep and schema != 'pg_temp':
            for name, df in dfs.items():
                if engine.has_table(name, schema):
                    engine.drop_table(name, schema)

def sql_query(engine, sql, *args, **kwargs):
    index = kwargs.pop('index', False)
    index_col = kwargs.pop('index_col', None)
    coerce_float = kwargs.pop('coerce_float', True)
    parse_dates = kwargs.pop('parse_dates', None)
    params = kwargs.pop('params', None)
    chunksize = kwargs.pop('chunksize', None)
    schema = kwargs.pop('schema', None)
    if_exists = kwargs.pop('if_exists', 'fail')
    keep = kwargs.pop('keep', False)
    create = kwargs.pop('keep', True)
    dfs = kwargs
    # set up variables
    args = list(args)
    # process extra arguments: dicts are data frames, lists/tuples are params
    for arg in args:
        if isinstance(arg, dict):
            dfs.update(arg)
        elif isinstance(arg, list) or isinstance(arg, tuple):
            params += list(arg)
        elif isinstance(arg, pandas.DataFrame):
            if getattr(arg, 'name') is not None:
                dfs[str(getattr(arg, 'name'))] = arg
            else:
                raise Exception("Dataframe given with no name: {}".format(arg))
        else:
            raise Exception("Invalid argument: {}".format(arg))

    try:
        # load the data frames into temp tables
        for name, df in dfs.items():
            # load the data into the table
            if not create and not engine.has_table(name, schema):
                raise Exception("Table {} does not exist!".format(name if schema is None else '.'.join([schema,name])))
            df.to_sql(name, engine, schema=schema, if_exists=if_exists, chunksize=chunksize, index=index)

        # run the query and return the results as a data frame
        return db.read_query(sql, index_col=index_col, coerce_float=coerce_float, parse_dates=parse_dates, params=params, chunksize=chunksize) if sql is not None else None
    finally:
        if not keep:
            for name, df in dfs.items():
                if engine.has_table(name, schema):
                    engine.drop_table(name, schema)

def query(engine, *args, **kwargs):
    if engine.dialect.name == 'postgresql':
        return pg_query(engine, *args, **kwargs)
    return sql_query(engine, *args, **kwargs)

def insert(engine, *args, **kwargs):
    return query(engine, None, *args, keep=True, create=False, if_exists='append', schema=None, **kwargs)

class Frame(pandas.DataFrame):
    _metadata = ['name']
    def __init__(self, cls, *args, **kwargs):
        name = kwargs.pop('name', None)
        super(Frame, self).__init__(*args, **kwargs)
        object.__setattr__(self, 'cls', cls)
        object.__setattr__(self, 'name', name)

    @property
    def _constructor(self):
        return self.cls

    def __finalize__(self, other, method=None, **kwargs):
        for name in self._metadata:
            object.frame.__setattr__(self, name, getattr(other.frame, name, None))
        return None

class Table(object):
    inflect_engine = inflect.engine()

    tablefmt = tabulate.TableFormat(
        lineabove=None,
        linebelowheader=tabulate.Line("", "-", "+", ""),
        linebetweenrows=None,
        linebelow=None,
        headerrow=tabulate.DataRow("", "|", ""),
        datarow=tabulate.DataRow("", "|", ""),
        padding=1, with_header_hide=None)

    def __init__(self, *args, **kwargs):
        object.__setattr__(self, 'frame', Frame(self.__class__, *args, **kwargs))

    def __call__(self, *args, **kwargs):
        self.frame._data = query(*args, **kwargs)._data
        return self

    def one(self):
        if len(self.frame) != 1:
            if len(self.frame) > 1: raise Exception("More than one record exists in data frame!")
            if len(self.frame) < 1: raise Exception("No records exist in data frame!")
        return self

    def get_one(self, name):
        return self.one().frame.iloc[0][name]

    def get_all(self, name):
        return self.frame[name]

    def __getattr__(self, name):
        singular = Table.inflect_engine.singular_noun(name)
        if name in self.frame.columns:
            if singular is not False and singular in self.frame.columns:
                raise Exception("Ambiguous field access: singular form \"{}\" and plural form \"{}\" both exist as columns".format(singular, name))
            return self.get_one(name)
        if singular is not False and singular in self.frame.columns:
            return self.get_all(singular)
        return getattr(self.frame, name)

    def __getitem__(self, name):
        if isinstance(name, int):
            if 0 <= name < len(self.frame): return self.frame.__getitem__(slice(name,name+1))
            else: raise IndexError("index {} is out of bounds".format(name))
        if not isinstance(name, str): return self.frame.__getitem__(name)
        singular = Table.inflect_engine.singular_noun(name)
        if name in self.frame.columns:
            if singular is not False and singular in self.frame.columns:
                raise Exception("Ambiguous field access: singular form \"{}\" and plural form \"{}\" both exist as columns".format(singular, name))
            return self.get_one(name)
        if singular is not False and singular in self.frame.columns:
            return self.get_all(singular)
        return self.frame.__getitem__(name)

    def set_one(self, name, value):
        self.one()
        return self.frame.__setitem__(name, value)

    def set_all(self, name, value):
        return self.frame.__setitem__(name, value)

    def __setattr__(self, name, value):
        singular = Table.inflect_engine.singular_noun(name)
        if name in self.frame.columns:
            return self.set_one(name, value)
        if singular is not False and singular in self.frame.columns:
            return self.set_all(singular, value)
        return self.frame.__setattr__(name, value)

    def __setitem__(self, name, value):
        if isinstance(name, int):
            if 0 <= name < len(self.frame): return self.frame.__setitem__(slice(name,name+1))
            else: raise IndexError("index {} is out of bounds".format(name))
        if not isinstance(name, str): return self.frame.__setitem__(name, value)
        singular = Table.inflect_engine.singular_noun(name)
        if name in self.frame.columns:
            if singular is not False and singular in self.frame.columns:
                raise Exception("Ambiguous field update: singular form \"{}\" and plural form \"{}\" both exist as columns".format(singular, name))
            return self.set_one(name, value)
        if singular is not False and singular in self.frame.columns:
            return self.set_all(singular, value)
        return self.frame.__setitem__(name, value)

    class Iter(object):
        def __init__(self, table):
            self.table = table
            self.i = 0
        def __iter__(self):
            return self
        def __next__(self):
            if self.i < len(self.table.frame):
                self.i += 1
                return self.table.frame[(self.i-1):self.i]
            raise StopIteration
        next = __next__
        
    def __iter__(self):
        return Table.Iter(self)

    def __repr__(self):
        if len(self.frame) == 0:
            return self.frame.__repr__()
        return tabulate.tabulate(
            self.frame,
            headers=['index' if self.frame.index.name is None else self.frame.index.name]+[str(c) for c in self.frame.columns],
            tablefmt=Table.tablefmt)

    def __finalize__(self, other, method=None, **kwargs):
        for name in self._metadata:
            object.frame.__setattr__(self, name, getattr(other.frame, name, None))
        return None

def _proxy(method):
    def fn(self, *args):
        return getattr(self.frame, method)(*args)
    return fn

# generate proxy methods for python magic methods. These can't be handled via __getattr__.
# see: http://www.rafekettler.com/magicmethods.html
for fn in (['cmp','eq','ne','lt','gt','le','ge','pos','neg','abs','invert','round','floor','ceil','trunc']+
           [x+r for x in ['','r','i'] for r in ['add','sub','mul','floordiv','div','truediv','mod','divmod','pow','lshift','rshift','and','or','xor']]+
           ['int','long','float','complex','oct','hex','index','trunc','coerce',
            'str','unicode','bytes','format','hash','nonzero','bool','dir','sizeof','delattr',
            'len','delitem','reversed','contains','missing','enter','exit']):
    if hasattr(pandas.DataFrame, '__'+fn+'__'):
        setattr(Table, '__'+fn+'__', _proxy('__'+fn+'__'))

def table(*args, **kwargs):
    args = list(args)
    mixins = []
    while len(args)>0 and isinstance(args[0], type):
        mixins.append(args.pop(0))
    if len(mixins)>0:
        return type('Table', tuple([Table]+mixins), {})(*args, **kwargs)
    return Table(*args, **kwargs)
    
if __name__ == '__main__':
    engine = sqlalchemy.create_engine("postgresql://eel/labtrack")
    df = pandas.DataFrame([{'a': 1, 'b': 2}])
    repr(table()(engine, "select * from df", df=df))
    
