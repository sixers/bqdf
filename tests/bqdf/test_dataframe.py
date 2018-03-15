import pytest
from pyspark import Row
from pyspark.sql.types import LongType, StringType, BooleanType, StructField, StructType
from tinyquery import tinyquery

from bqdf.context.tinyquery import TinyQueryContext
from bqdf import functions as F


@pytest.fixture(scope='session')
def context():
    return TinyQueryContext()


@pytest.fixture(scope='session')
def sample_dataframe(context: TinyQueryContext):
    return context.createDataFrame([
        Row(a=1, b="x"),
        Row(a=2, b="z"),
        Row(a=3, b="z"),
    ], StructType([
        StructField('a', LongType()),
        StructField('b', StringType()),
    ]))


@pytest.fixture(scope='session')
def sample_table(sample_dataframe):
    table_name = 'sample_table'
    sample_dataframe.write.saveAsTable(table_name)
    yield sample_dataframe.context.read.table(table_name)
    sample_dataframe.context.deleteTable(table_name)


def test_count(sample_table):
    assert sample_table.count() == 3


@pytest.mark.parametrize("select,expected", [
    (('*',), [
        Row(a=1, b="x"),
        Row(a=2, b="z"),
        Row(a=3, b="z"),
    ]),
    (('a', 'b'), [
        Row(a=1, b="x"),
        Row(a=2, b="z"),
        Row(a=3, b="z"),
    ]),
    ((F.col('a'), F.col('b')), [
        Row(a=1, b="x"),
        Row(a=2, b="z"),
        Row(a=3, b="z"),
    ]),
    (((F.col('a') * F.lit(2)).alias('z'),), [
        Row(z=2),
        Row(z=4),
        Row(z=6),
    ]),
    ((F.col('a').alias('x'), F.col('b').alias('y')), [
        Row(x=1, y="x"),
        Row(x=2, y="z"),
        Row(x=3, y="z"),
    ]),
])
def test_simple_select(sample_table, select, expected):
    assert sample_table.select(*select).collect() == expected


# TODO: Find and fix bug
def evaluate_ColumnRef(self, column_ref, ctx):
    try:
        return ctx.columns[(column_ref.table, column_ref.column)]
    except KeyError:
        return ctx.columns[(None, column_ref.column)]


tinyquery.evaluator.Evaluator.evaluate_ColumnRef = evaluate_ColumnRef


@pytest.mark.parametrize("where,expected", [
    (F.col('a') == F.lit(2), [
        Row(a=2, b="z"),
    ]),
    (F.col('a') >= F.lit(2), [
        Row(a=2, b="z"),
        Row(a=3, b="z"),
    ]),
])
def test_simple_where(sample_table, where, expected):
    print(expected)
    assert sample_table.where(where).collect() == expected


@pytest.mark.parametrize("aggs,expected", [
    ([F.countDistinct('b').alias('d')], [
        Row(d=2),
    ]),
    ([F.sum('a').alias('s')], [
        Row(s=6),
    ]),
])
def test_simple_select_aggregations(sample_table, aggs, expected):
    print(expected)
    assert sample_table.select(*aggs).collect() == expected


@pytest.mark.parametrize("groupby,aggs,expected", [
    (['b'], [F.sum('a').alias('s')], [
        Row(b='x', s=1),
        Row(b='z', s=5),
    ]),
    (['b'], [F.max('a').alias('m'), F.sum('a').alias('s')], [
        Row(b='x', m=1, s=1),
        Row(b='z', m=3, s=5),
    ]),
    (['b'], [F.collect_list('a').alias('c')], [
        Row(b='x', c=[1]),
        Row(b='z', c=[2, 3]),
    ]),
])
def test_simple_groupby_aggregations(sample_table, groupby, aggs, expected):
    print(expected)
    assert sample_table.groupby(*groupby).agg(*aggs).collect() == expected


def test_complex_query(sample_table):
    df = sample_table \
        .groupby('b') \
        .agg(F.sum('a').alias('s'), F.max('a').alias('m')) \
        .where(F.col('s') == F.col('m')) \
        .select(F.col('b'), (F.col('m') + F.col('s')).alias('c'))

    assert df.collect() == [Row(b='x', c=2)]


def test_order_by(sample_table):
    assert sample_table.orderBy('a', ascending=False).collect() == [
        Row(a=3, b="z"),
        Row(a=2, b="z"),
        Row(a=1, b="x"),
    ]


@pytest.fixture(scope='session')
def join_table(context):
    df = context.createDataFrame([
        Row(a=1, c=7, d="a"),
        Row(a=2, c=9, d="b"),
        Row(a=2, c=11, d="c"),
    ], StructType([
        StructField('a', LongType()),
        StructField('c', LongType()),
        StructField('d', StringType()),
    ]))
    table_name = 'join_table'
    df.write.saveAsTable(table_name)
    yield context.read.table(table_name)
    context.deleteTable(table_name)


def test_joins(sample_table, join_table):
    assert sample_table.join(join_table, on='a', how='inner').collect() == [
        Row(a=1, b="x", c=7, d="a"),
        Row(a=2, b="z", c=9, d="b"),
        Row(a=2, b="z", c=11, d="c"),
    ]
