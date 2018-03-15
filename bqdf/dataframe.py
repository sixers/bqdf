from pypika import Query, Order, JoinType
from pypika import functions as fn
from pyspark import Row

from bqdf.functions import _ensure_cols, count
from bqdf.types import Column


class DataFrame:
    def __init__(self, context):
        self.context = context


    @property
    def schema(self):
        raise NotImplementedError()

    @property
    def write(self):
        return self.context.writer(self)

    @property
    def read(self):
        return self.context.reader(self)


def _resolve_terms(*cols):
    return [col.term if isinstance(col, Column) else col for col in cols]


class QueryDataFrame(DataFrame):
    def __init__(self, context, from_):
        super().__init__(context)
        self.from_ = from_

    def collect(self):
        query = Query.from_(self.from_).select('*')
        print("collect", query.get_sql())
        result = self._execute(query)

        columns = [c[1] for c in result.columns.keys()]
        RowSpec = Row(*columns)

        rows = []
        for i in range(result.num_rows):
            values = [spec.values[i] for spec in result.columns.values()]
            rows.append(RowSpec(*values))

        return rows

    @property
    def query(self):
        return Query.from_(self.from_).select('*')

    def count(self):
        result = self.select(count('*').alias('c1')).collect()
        return result[0].c1

    def toPandas(self):
        raise NotImplementedError()

    def distinct(self):
        raise NotImplementedError()

    def dropDuplicates(self, subset=None):
        raise NotImplementedError()

    def where(self, condition):
        query = Query.from_(self.from_).select('*').where(*_resolve_terms(condition))
        print(query)
        return QueryDataFrame(self.context, query)

    def groupby(self, *exprs):
        return GroupedData(self.context, self.from_, _ensure_cols(exprs))

    def join(self, other, on=None, how=None):
        join_types = {
            'left':  JoinType.left,
            'right': JoinType.right,
            'inner': JoinType.inner,
        }
        # todo: assert other is QueryDataFrame
        query = self.query.join(other.query, how=join_types.get(how)).on_field(on)#

        print("here1111", query._joins[0].item.alias)
        # .select('*')
        return QueryDataFrame(self.context, query)

    def orderBy(self, *cols, ascending=None):
        query = Query.from_(self.from_).select('*').orderby(*cols, order=Order.asc if ascending else Order.desc)
        return QueryDataFrame(self.context, query)

    def select(self, *cols):
        query = Query.from_(self.from_).select(*_resolve_terms(*cols))
        return QueryDataFrame(self.context, query)

    def _execute(self, query):
        result = self.context.sql(query.get_sql())
        return result


class GroupedData:
    def __init__(self, context, from_, groupby):
        self.from_ = from_
        self.groupby = groupby
        self.context = context

    def agg(self, *exprs):
        exprs = _resolve_terms(*self.groupby) + _resolve_terms(*exprs)

        query = Query.from_(self.from_)
        for g in self.groupby:
            query = query.groupby(*_resolve_terms(g))
        query = query.select(*exprs)

        return QueryDataFrame(self.context, query)


class LocalDataFrame(DataFrame):
    def __init__(self, context, records, schema):
        super().__init__(context)
        self.records = records
        self._schema = schema

    @property
    def schema(self):
        return self._schema

    def collect(self):
        return self.records