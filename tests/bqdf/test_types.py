import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType

from bqdf.types import convert_to_bigquery_schema


@pytest.fixture
def spark_schema():
    return StructType([
        StructField('i', IntegerType(), False),
        StructField('rr', ArrayType(StructType([
            StructField('inner_non_repeated', StringType(), True),
            StructField('inner_repeated', ArrayType(StringType()), True),
        ])), True),
        StructField('r', StructType([
            StructField('s', StringType(), True),
            StructField('inner_repeated', ArrayType(StringType()), True),
            StructField('r2', StructType([
                StructField('d2', IntegerType(), True)
            ]), True)
        ]), True)
    ])

@pytest.fixture
def bigguery_schema():
    return {
        'fields': [
            {
                'name': 'i',
                'type': 'INTEGER',
                'mode': 'REQUIRED',
            },
            {
                'name': 'rr',
                'type': 'RECORD',
                'mode': 'REPEATED',
                'fields': [
                    {
                        'name': 'inner_non_repeated',
                        'type': 'STRING',
                        'mode': 'NULLABLE',
                    },
                    {
                        'name': 'inner_repeated',
                        'type': 'STRING',
                        'mode': 'REPEATED',
                    },
                ],
            },
            {
                'name': 'r',
                'type': 'RECORD',
                'mode': 'NULLABLE',
                'fields': [
                    {
                        'name': 's',
                        'type': 'STRING',
                        'mode': 'NULLABLE',
                    },
                    {
                        'name': 'inner_repeated',
                        'type': 'STRING',
                        'mode': 'REPEATED',
                    },
                    {
                        'name': 'r2',
                        'type': 'RECORD',
                        'mode': 'NULLABLE',
                        'fields': [
                            {
                                'name': 'd2',
                                'type': 'INTEGER',
                                'mode': 'NULLABLE',
                            },
                        ],
                    },
                ],
            },
        ],
    }


def test_convert_to_bigquery_schema(spark_schema, bigguery_schema):
    assert convert_to_bigquery_schema(spark_schema) == bigguery_schema