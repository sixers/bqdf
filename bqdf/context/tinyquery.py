import json

from tinyquery import tinyquery

from bqdf.dataframe import LocalDataFrame, QueryDataFrame
from bqdf.types import convert_to_bigquery_schema, convert_row_to_dict


class TinyQueryContext:
    def __init__(self):
        self.tq = tinyquery.TinyQuery()

    def createDataFrame(self, records, schema):
        return LocalDataFrame(self, records, schema)

    def writer(self, df):
        return TinyQueryDataFrameWriter(self, df)

    def reader(self):
        return TinyQueryDataFrameReader(self)

    @property
    def read(self):
        return self.reader()

    def deleteTable(self, table_name):
        del self.tq.tables_by_name[table_name]

    def sql(self, sql):
        print(sql)
        return self.tq.evaluate_query(sql)


class TinyQueryDataFrameWriter:
    def __init__(self, context, df):
        self.context = context
        self.df = df

    def saveAsTable(self, name):
        schema = convert_to_bigquery_schema(self.df.schema)
        records = map(json.dumps, map(convert_row_to_dict, self.df.collect()))

        self.context.tq.load_table_from_newline_delimited_json(
            name,
            json.dumps(schema['fields']),
            list(records)
        )

        return QueryDataFrame(self.context, name)


class TinyQueryDataFrameReader:
    def __init__(self, context):
        self.context = context

    def table(self, table_name):
        return QueryDataFrame(self.context, table_name)
