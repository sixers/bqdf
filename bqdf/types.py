import operator
from collections import OrderedDict

from base_spark.utils.sql.types import RowHelper, StructField
from pyspark.sql.types import StructType, IntegerType, StringType, ArrayType, LongType, BooleanType
from tinyquery.tq_types import TYPE_SET
from pypika import functions as fn, Field


BIGQUERY_FIELD_MAPPING = {
    IntegerType: 'INTEGER',
    LongType: 'INTEGER',
    BooleanType: 'BOOLEAN',
    StringType: 'STRING',
    StructType: 'RECORD',
}


REVERSED_BIGQUERY_FIELD_MAPPING = {
    'INTEGER': LongType,
    'BOOLEAN': BooleanType,
    'STRING': StringType,
    'RECORD': StructType,
}


def _convert_to_spark_schema(schema: dict):

    schema = schema.copy()
    fields = OrderedDict()

    for field in schema['fields']:
        name, type, mode = field['name'], field['type'], field['mode']
        if '.' in name:
            parts = name.split(".")
            _fields = fields
            for part in parts[:-1]:
                if part not in _fields:
                    _fields[part] = {
                        'type': 'RECORD',
                        'nullable': 'NULLABLE', ## ???
                        'fields': OrderedDict(),
                    }
                    _fields = _fields[part]['fields']
            _fields[parts[-1]] = {
                'type': type,
                'mode': mode,
            }
        else:
            fields[name] = {
                'type': type,
                'mode': mode,
            }

    struct_fields = []
    for name, spec in fields.items():
        type = REVERSED_BIGQUERY_FIELD_MAPPING.get(spec['type'])
        if spec['mode'] == 'REPEATED':
            type = ArrayType(type)

        struct_field = StructField(name, type,  spec['mode'] == 'NULLABLE')

        struct_fields.append(struct_field)

    return StructType(*struct_fields)


def convert_to_bigquery_schema(schema: StructType):
    fields = []
    for field in schema.fields:
        if isinstance(field.dataType, ArrayType):
            type = field.dataType.elementType
            mode = 'REPEATED'
        elif field.nullable:
            type = field.dataType
            mode = 'NULLABLE'
        else:
            type = field.dataType
            mode = 'REQUIRED'

        bigquery_field = {
            'name': field.name,
            'type': BIGQUERY_FIELD_MAPPING[type.__class__],
            'mode': mode,
        }

        if isinstance(type, StructType):
            bigquery_field['fields'] = convert_to_bigquery_schema(type)['fields']

        fields.append(bigquery_field)

    print(fields)

    return {'fields': fields}


def convert_row_to_dict(row):
    return RowHelper.toDict(row, ordered=False)


def _bin_op(op):
    def _(self, other):
        return Column(op(self.term, other.term))
    return _


def _func_op(op):
    def _(self):
        return Column(op(self.term))
    return _


class Column:
    # arithmetic operators
    __neg__ = _func_op(operator.neg)
    __add__ = _bin_op(operator.add)
    __sub__ = _bin_op(operator.sub)
    __mul__ = _bin_op(operator.mul)
    __truediv__ = _bin_op(operator.truediv)
    __mod__ = _bin_op(operator.mod)
    # __rmod__ = _reverse_op("mod")
    # __pow__ = _bin_func_op("pow")
    # __rpow__ = _bin_func_op("pow", reverse=True)

    # logistic operators
    __eq__ = _bin_op(operator.eq)
    __ne__ = _bin_op(operator.ne)
    __lt__ = _bin_op(operator.lt)
    __le__ = _bin_op(operator.le)
    __ge__ = _bin_op(operator.ge)
    __gt__ = _bin_op(operator.gt)

    # boolean operators
    __and__ = _bin_op(operator.and_)
    __or__ = _bin_op(operator.or_)
    __invert__ = _func_op(operator.not_)
    # __rand__ = _bin_op("and")
    # __ror__ = _bin_op("or")

    def __init__(self, term):
        self.term = term

    def alias(self, name):
        return Column(self.term.as_(name))

    def cast(self, dataType):
        return Column(fn.Cast(self.term, BIGQUERY_FIELD_MAPPING[dataType.__class__]))


    def __repr__(self):
        return repr(self.term)

    def __str__(self):
        return str(self.term)