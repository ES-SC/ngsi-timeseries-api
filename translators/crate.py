from crate import client
from datetime import datetime, timedelta
from translators.base_translator import BaseTranslator
from utils.common import iter_entity_attrs
import statistics
import warnings


# NGSI TYPES: Not properly documented so this might change. Based on experimenting with Orion.
# CRATE TYPES: https://crate.io/docs/reference/sql/data_types.html
_CRATE_STRING = 'string'

NGSI_TO_CRATE = {
    "Text": _CRATE_STRING,
    "string": _CRATE_STRING,
    "Number": 'float',
    "Integer": 'long',
    "Boolean": 'boolean',
    "DateTime": 'timestamp',
    "json:geo": 'geo_shape',
}
CRATE_TO_NGSI = dict((v, k) for (k,v) in NGSI_TO_CRATE.items())
# Consequence of not having well documented NGSI TYPES, I'm leeping consistency for unittesting.
CRATE_TO_NGSI['string'] = 'Text'


class CrateTranslator(BaseTranslator):

    def __init__(self, host, port=4200, db_name="ngsi-tsdb"):
        super(CrateTranslator, self).__init__(host, port, db_name)


    def setup(self):
        self.conn = client.connect(["{}:{}".format(self.host, self.port)], error_trace=True)
        self.cursor = self.conn.cursor()


    def dispose(self):
        self.cursor.close()
        self.conn.close()


    def _refresh(self, entity_types):
        """
        Used for testing purposes only!
        :param tables: str (comma separated list of table names)
        """
        table_names = ','.join([self._et2tn(et) for et in entity_types])
        self.cursor.execute("refresh table {}".format(table_names))


    def _get_isoformat(self, ms_since_epoch):
        """
        :param ms_since_epoch:
            As stated in CrateDB docs: Timestamps are always returned as long values (ms from epoch).
        :return: str
            The equivalent datetime in ISO 8601.
        """
        if ms_since_epoch is None:
            raise ValueError
        utc = datetime(1970, 1, 1, 0, 0, 0, 0) + timedelta(milliseconds=ms_since_epoch)
        return utc.isoformat()


    def translate_to_ngsi(self, resultset, keys, table_name):
        stmt = "select column_name, data_type from information_schema.columns where table_name = '{}'".format(table_name)
        self.cursor.execute(stmt)
        res = self.cursor.fetchall()
        table_columns = dict((k, v) for (k, v) in res)

        for r in resultset:
            entity = {}
            for k, v in zip(keys, r):
                if k == self.TIME_INDEX_NAME:
                    # TODO: This might not be valid NGSI. Should we include this time_index in the reply?.
                    # If so, shouldn't it have metadata?
                    entity[self.TIME_INDEX_NAME] = self._get_isoformat(v)
                elif k == 'entity_type':
                    entity['type'] = v
                elif k == 'entity_id':
                    entity['id'] = v
                else:
                    t = CRATE_TO_NGSI[table_columns[k]]
                    entity[k] = {'value': v, 'type': t}
                    if t == 'DateTime' and entity[k]['value']:
                        entity[k]['value'] = self._get_isoformat(entity[k]['value'])
            yield entity


    def _et2tn(self, entity_type):
        return "et{}".format(entity_type)

    def _tn2et(self, table_name):
        return table_name[2:]


    def insert(self, entities):
        if not isinstance(entities, list):
            raise TypeError("Entities expected to be of type list, but got {}".format(type(entities)))

        tables = {}             # {table_name -> {column_name -> crate_column_type}}
        entities_by_tn = {}   # {table_name -> list(entities)}

        # TEMP DEMO HACK
        entity_types = [e['type'] for e in entities]
        if 'AirQualityObserved' in entity_types or 'TrafficFlowObserved' in entity_types:
            return self._demo_hack(entities)

        # Collect tables info
        for e in entities:
            tn = self._et2tn(e['type'])

            table = tables.setdefault(tn, {})
            entities_by_tn.setdefault(tn, []).append(e)

            if self.TIME_INDEX_NAME not in e:
                # Recall it's the reporter's job to ensure each entity comes with a TIME_INDEX attribute.
                warnings.warn("Translating entity without TIME_INDEX. {}".format(e))

            table['entity_id'] = NGSI_TO_CRATE['Text']  # We intentionally avoid using id as a column name.
            table['entity_type'] = NGSI_TO_CRATE['Text']  # We intentionally avoid using type as a column name.
            for attr in iter_entity_attrs(e):
                if attr == self.TIME_INDEX_NAME:
                    table[self.TIME_INDEX_NAME] = NGSI_TO_CRATE['DateTime']
                else:
                    ngsi_t = e[attr]['type']
                    if ngsi_t not in NGSI_TO_CRATE:
                        # Treat unsupported types as string
                        warnings.warn('Unsupported NGSI type: {}. Treating it as string.'.format(ngsi_t))
                        crate_t = _CRATE_STRING
                    else:
                        crate_t = NGSI_TO_CRATE[ngsi_t]
                    table[attr] = crate_t

        # Create tables
        for tn, table in tables.items():
            columns = ','.join('{} {}'.format(cn, ct) for cn, ct in table.items())
            stmt = "create table if not exists {} ({})".format(tn, columns)
            self.cursor.execute(stmt)

        # Make inserts
        for tn, entities in entities_by_tn.items():
            col_names = sorted(tables[tn].keys())

            entries = []    # raw values in same order as column names for all entities
            for e in entities:
                temp = e.copy()
                temp['entity_type'] = {'value': temp.pop('type')}  # The type is saved in table
                temp['entity_id'] = {'value': temp.pop('id')}
                temp[self.TIME_INDEX_NAME] = {'value': temp[self.TIME_INDEX_NAME]}
                try:
                    values = []
                    for x in col_names:
                        val = temp[x]['value']
                        # Treat unsupported types as string
                        if tables[tn][x] == _CRATE_STRING and not isinstance(val, str):
                            val = '{}'.format(val)
                        values.append(val)
                except KeyError as e:
                    msg = "Seems like not all entities of same type came with the same set of attributes. {}".format(e)
                    raise NotImplementedError(msg)
                entries.append(values)

            stmt = "insert into {} ({}) values ({})".format(tn, ', '.join(col_names), ('?,' * len(col_names))[:-1])
            self.cursor.executemany(stmt, entries)

        return self.cursor


    def _get_table_names(self):
        self.cursor.execute("select table_name from information_schema.tables where table_schema = 'doc'")
        return [r[0] for r in self.cursor.rows]


    def query(self, attr_names=None, entity_type=None, entity_id=None, where_clause=None):
        if entity_id and not entity_type:
            raise NotImplementedError("For now you must specify entity_type when stating entity_id")

        select_clause = "{}".format(attr_names[0]) if attr_names else "*"  # TODO: support some attrs
        if not where_clause:
            # TODO: support entity_id filter with custom where clause
            where_clause = "where entity_id = '{}'".format(entity_id) if entity_id else ''

        if entity_type:
            table_names = [self._et2tn(entity_type)]
        else:
            table_names = self._get_table_names()

        all_entities = []
        for tn in table_names:
            op = "select {} from {} {}".format(select_clause, tn, where_clause)
            self.cursor.execute(op)
            res = self.cursor.fetchall()
            col_names = [x[0] for x in self.cursor.description]
            entities = list(self.translate_to_ngsi(res, col_names, tn))
            all_entities.extend(entities)
        return all_entities


    def average(self, attr_name, entity_type=None, entity_id=None):
        if entity_id and not entity_type:
            raise NotImplementedError("For now you must specify entity_type when stating entity_id")

        if entity_type:
            table_names = [self._et2tn(entity_type)]
        else:
            # The semantic correctness of this operation is up to the client.
            table_names = self._get_table_names()

        values = []
        for tn in table_names:
            select_clause = "avg({})".format(attr_name)
            where_clause = ("where entity_id = '%s'" % entity_id) if entity_id else ""
            stmt = "select {} from {} {}".format(select_clause, tn, where_clause)
            self.cursor.execute(stmt)
            avg = self.cursor.fetchone()[0]
            values.append(avg)
        return statistics.mean(values)


    def _demo_hack(self, entities):
        """
        Custom insert of TrafficFlowObserved and AirQualityObserved
        """
        # Create table trafficairdata
        table_name = 'trafficairdata'
        column_names = ['created_at', 'h', 'id', 'latitude', 'longitude', 'no2', 'p', 'pm10', 'pm25', 't', 'type', 'value']
        column_types = ['timestamp',  'float', 'integer', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'string', 'float']
        assert len(column_names) == len(column_types)

        columns = ','.join('{} {}'.format(n,t) for n,t in zip(column_names, column_types))
        stmt = "create table if not exists {} ({})".format(table_name, columns)
        self.cursor.execute(stmt)

        entries = []
        for e in entities:
            if e['type'] == 'AirQualityObserved':
                lat, long = e['location']['value'].split(',')
                entries.append((
                    e.get(BaseTranslator.TIME_INDEX_NAME),
                    e['h']['value'] if 'h' in e else None,
                    e.get('id'),
                    lat,
                    long,
                    e['no2']['value'] if 'no2' in e else None,
                    e['p']['value'] if 'p' in e else None,
                    e['pm10']['value'] if 'pm10' in e else None,
                    e['pm25']['value'] if 'pm25' in e else None,
                    e['t']['value'] if 't' in e else None,
                    'environment',
                    e['aqi']['value'] if 'aqi' in e else None,
                ))
            elif e['type'] == 'TrafficFlowObserved':
                lat, long = e['location']['value']['coordinates'][0]
                entries.append((
                    e[BaseTranslator.TIME_INDEX_NAME],
                    None,
                    e.get('id'),
                    lat,
                    long,
                    None,
                    None,
                    None,
                    None,
                    None,
                    'traffic',
                    e['intensity']['value'],
                ))
            else:
                continue

        stmt = "insert into {} ({}) values ({})".format(table_name, ', '.join(column_names), ('?,' * len(column_names))[:-1])
        return self.cursor.executemany(stmt, entries)

