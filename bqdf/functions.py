from pypika import functions as fn, Field
from pypika.terms import ValueWrapper, AggregateFunction
from tinyquery import context, tq_modes
from tinyquery.runtime import _AGGREGATE_FUNCTIONS, AggregateFunction as AggregateFunctionDef

from bqdf.types import Column


# Pypika
# TODO: Not implemented in TinyQuery
class NestFunction(AggregateFunctionDef):
    def check_types(self, arg):
        return arg

    def _evaluate(self, num_rows, column):
        values = [[arg for arg in column.values]]
        return context.Column(type=column.type,
                              mode=tq_modes.REPEATED,
                              values=values)


_AGGREGATE_FUNCTIONS['nest'] = NestFunction()


# TODO: Not implemented in TinyQuery
class Nest(AggregateFunction):
    def __init__(self, term, alias=None):
        super(Nest, self).__init__('NEST', term, alias=alias)


# Spark
def _ensure_col(c):
    if not isinstance(c, Column):
        return col(c)
    else:
        return c


def _ensure_cols(cols):
    return list(map(_ensure_col, cols))


def count(param):
    return Column(fn.Count(param))


def col(name):
    return Column(Field(name))


def lit(value):
    return Column(ValueWrapper(value))


def countDistinct(param):
    return Column(fn.Count(_ensure_col(param).term).distinct())


def sum(param):
    return Column(fn.Sum(_ensure_col(param).term))


def max(param):
    return Column(fn.Max(_ensure_col(param).term))


def collect_list(param):
    return Column(Nest(_ensure_col(param).term))
