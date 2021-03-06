import json
import operator
import os
import random
#from pyspark.sql.functions import lit
from hdfs import InsecureClient
from pyspark.sql import DataFrame
from pyspark.sql import Column

gctf_data_path='hdfs://spark-master0-dsl05:9000/gctf_data' #'/home/sprk/shared/gctf_data'
gctf_data_path_no_url='/gctf_data'
full_tensor_name='gtp_full_tensor'
    
def iter_indices_gen_data(all_tensors_config, cardinalities, tensor_name, fd, zero_based_indices, iter_index_values=[]):
    tensor_index_names = all_tensors_config[tensor_name]['indices']
    if len(iter_index_values) == len(tensor_index_names):
        for iiv_index, iiv in enumerate(iter_index_values):
            if iiv_index != 0:
                fd.write(',')
            if zero_based_indices:
                iiv_str = str( iiv )
            else:
                iiv_str = str( iiv + 1 )
            fd.write( iiv_str )
        fd.write( ',%.5f\n' %random.random() )
    else:
        iter_index_name = tensor_index_names[ len(iter_index_values) ]
        for index_val in range(cardinalities[iter_index_name]):
            iter_indices_gen_data( all_tensors_config, cardinalities, tensor_name, fd, zero_based_indices, iter_index_values+[index_val] )

def write_header(all_tensors_config, tensor_name, fd):
    # print header
    for index_index, index_name in enumerate(all_tensors_config[tensor_name]['indices']):
        if index_index != 0:
            fd.write(',')
        fd.write(index_name)
    fd.write(',value\n')

def generate_random_tensor_data_local(all_tensors_config, cardinalities, tensor_name, zero_based_indices=False):
    # generate tensor data on local file

    local_filename=os.path.join('/tmp', tensor_name+'.csv')
    print ('generate_random_tensor_data_local: generating %s' %local_filename)
    assert not os.path.exists(local_filename), 'data file %s exists can not procede' %local_filename

    fd = open(local_filename, 'w')
    write_header(all_tensors_config, tensor_name, fd)
    iter_indices_gen_data(all_tensors_config, cardinalities, tensor_name, fd, zero_based_indices)
    fd.close()

    all_tensors_config[tensor_name]['local_filename'] = local_filename

def generate_random_tensor_data_hdfs(all_tensors_config, cardinalities, tensor_name, zero_based_indices=False, hdfs_url='http://spark-master0-dsl05:50070',
hdfs_user='sprk'):
    # generate tensor data on local file

    hdfs_filename=os.path.join(gctf_data_path_no_url, tensor_name+'.csv')
    print ('generate_random_tensor_data_hdfs: generating %s' %hdfs_filename)

    client = InsecureClient(hdfs_url, user=hdfs_user)
    assert client.status(hdfs_filename, strict=False) is None, 'data file %s exists can not procede' %hdfs_filename

    with client.write(hdfs_filename, encoding='utf-8') as writer:
        write_header(all_tensors_config, tensor_name, writer)
        iter_indices_gen_data(all_tensors_config, cardinalities, tensor_name, writer, zero_based_indices)
        # fd.close() # TODO: hdfs client api does not specify close?

    all_tensors_config[tensor_name]['hdfs_filename'] = hdfs_filename

def add_tensor_to_all_tensors(all_tensors_config, tensor_name, indices, cardinalities):
    all_tensors_config[tensor_name] = {
        'indices' : indices,
        'numel' : reduce(operator.mul, [cardinalities[i] for i in indices])
    }

def generate_local_tensor_data(all_tensors_config, cardinalities, tensor_name, indices):
    assert tensor_name not in all_tensors_config, 'tensor_name %s already exists in all_tensors_config %s' %(tensor_name, all_tensors_config)
    add_tensor_to_all_tensors(all_tensors_config, tensor_name, indices, cardinalities)
    generate_random_tensor_data_local(all_tensors_config, cardinalities, tensor_name)

def generate_hdfs_tensor_data(all_tensors_config, cardinalities, tensor_name, indices):
    assert tensor_name not in all_tensors_config, 'tensor_name %s already exists in all_tensors_config %s' %(tensor_name, all_tensors_config)
    add_tensor_to_all_tensors(all_tensors_config, tensor_name, indices, cardinalities)
    generate_random_tensor_data_hdfs(all_tensors_config, cardinalities, tensor_name)

def get_all_indices(all_tensors_config):
    all_indices = set()
    for tensor_name, tensor in all_tensors_config.iteritems():
        all_indices.update( tensor['indices'] )
    # all_indices must have fixed ordering
    return list(sorted(all_indices))

def read_tensor_data_from_hdfs(spark_session, all_tensors_config, tensor_name, root_path):
    tensor_def = all_tensors_config[tensor_name]
    hdfs_filename = tensor_def['hdfs_filename'] # TODO assert for hdfs file existance
    print( 'read_tensor_data_from_hdfs: for tensor %s using file %s' %(tensor_name, hdfs_filename) )

    schema = ''
    for index_index, index in enumerate(tensor_def['indices']):
        if index_index != 0:
            schema += ','
        schema += index + ' INT '
    schema += ', value DOUBLE'

    print('read_tensor_data: reading file %s with schema %s' %(hdfs_filename, schema))

    tensor_df = spark_session.read.load(hdfs_filename, format="csv", sep=",", header="true", schema=schema, numPartitions=58) #, inferSchema="true")
    #tensor_def['data_local'] = tensor_data.collect() # TODO: remove collect dependency (working on same tensors to produce output does not work with map function) -> use single rdd with tensor data annotated with tensor name

    # # add missing columns
    # missing_columns = set(get_all_indices(all_tensors_config))-set(tensor_def['indices'])
    # for mc in missing_columns:
    #     tensor_data = tensor_data.withColumn(mc, lit(None))

    tensor_df.cache()
    tensor_df.checkpoint()
    return tensor_df

def linear_index_to_DOK_index(linear_index, tensor_indices, all_cardinalities):
    # linear index calculated per tensor using tensor's indices
    current_stride = 1
    DOK_indices = {}

    for index_name in tensor_indices:
        index_cardinality = all_cardinalities[index_name]
        value = ( (linear_index / current_stride) % index_cardinality ) + 1
        current_stride *= index_cardinality
        DOK_indices[index_name] = value

    return DOK_indices
    
# def linear_index_to_tensor_index( all_tensors, all_cardinalities, linear_index, linear_index_tensor_name, tensor_index_tensor_name=None, as_dict=True ):
#     current_stride = 1
#     tensor_index_values_dict = {}
#     if tensor_index_tensor_name is None:
#         tensor_index_tensor_name = linear_index_tensor_name

#     tensor_index_tensor_indices = all_tensors[tensor_index_tensor_name]['indices']
#     for index_name in all_tensors[linear_index_tensor_name]['indices']:
#         index_cardinality = all_cardinalities[index_name]
#         value = ((linear_index / current_stride) % index_cardinality) + 1
#         current_stride *= index_cardinality
#         if index_name in tensor_index_tensor_indices:
#             tensor_index_values_dict[index_name] = value

#     if as_dict:
#         return tensor_index_values_dict
#     else:
#         # make sure ordering is in 'tensor_index_tensor_name' indices order
#         tensor_index_values_list = []
#         for index_name in tensor_index_tensor_indices:
#             tensor_index_values_list.append(tensor_index_values_dict[index_name])
#         return tuple(tensor_index_values_list)

class ComplexEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, Column):
            return str(obj)
        elif obj == operator.mul:
            return '*'
        elif obj == operator.truediv:
            return '/'
        elif obj == operator.add:
            return '+'
        elif obj == operator.sub:
            return '-'
        elif isinstance(obj, DataFrame):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

# # from https://stackoverflow.com/q/354038/1056345
# def is_number(s):
#     try:
#         float(s)
#         return True
#     except ValueError:
#         return False
# #################################################

def get_observed_tensor_names_of_latent_tensor(gctf_model, ltn):
    otn_with_ltn=[]
    for factorization in gctf_model['config']['factorizations']:
        if ltn in factorization['latent_tensors']:
            otn_with_ltn.append(factorization['observed_tensor'])

    assert len(otn_with_ltn), ('ltn %s was not found in any of the factorizations' %ltn)
    return otn_with_ltn


def gengtp(gctf_model, output_tensor_name, input_tensor_names, full_tensor_name_str=None):
    gtp_spec = {
        'config' : {
            'cardinalities' : gctf_model['config']['cardinalities'],
            'output' : output_tensor_name,
            'input' : input_tensor_names
        },
        'tensors' : {
            output_tensor_name : gctf_model['tensors'][output_tensor_name]
        },
    }

    if full_tensor_name_str:
        gtp_spec['tensors']['gtp_full_tensor'] = gctf_model['tensors'][full_tensor_name_str]

    for itn in input_tensor_names:
        gtp_spec['tensors'][itn] = gctf_model['tensors'][itn]

    gtp_str = '%s <- gtp(%s)' % (output_tensor_name, ', '.join(input_tensor_names))
    return (gtp_spec, gtp_str)


def get_hadamard_pre_processor_str(pre_processor_str):
    if pre_processor_str['operator'] == 'pow':
        return 'pow%s' %pre_processor_str['argument']
    elif pre_processor_str['operator'] == 'log':
        return 'log'
    else:
        raise Exception('unknown pre_processor %s' %pre_processor_str)


def get_hadamard_str(input_spec, output_name, level=0):
    if level == 0:
        rule_str = '%s <- ' %output_name
    else:
        rule_str = ''

    for argument_index, argument in enumerate(input_spec['arguments']):
        if 'suboperation' in argument:
            rule_str += ' (%s) ' %get_hadamard_str(argument['suboperation'], output_name, level+1)
        else:
            if 'pre_processor' in argument:
                rule_str += get_hadamard_pre_processor_str(argument['pre_processor'])

            rule_str += ' %s ' %(argument['data'])
            if argument_index < len(input_spec)-1:
                rule_str += '%s ' %(json.dumps( input_spec['combination_operator'], cls=ComplexEncoder ).strip('"'))

    return rule_str


def genhadamard(rule):
    rule['operation_str'] = get_hadamard_str(rule['input'], rule['output'])
    return rule


def create_full_tensor(spark_session, all_tensors_config, all_cardinalities):
    print('gtp: creating full tensor with indices %s' %all_cardinalities)
    generate_hdfs_tensor_data(all_tensors_config, all_cardinalities, full_tensor_name, all_cardinalities.keys())
    all_tensors_config[full_tensor_name]['df'] = read_tensor_data_from_hdfs(spark_session, all_tensors_config, full_tensor_name, gctf_data_path)


def generate_spark_tensor(spark_session, all_tensors_config, all_cardinalities, new_tensor_name, new_tensor_indices):
    generate_hdfs_tensor_data(all_tensors_config, all_cardinalities, new_tensor_name, new_tensor_indices)
    all_tensors_config[new_tensor_name]['df'] = read_tensor_data_from_hdfs(spark_session, all_tensors_config, new_tensor_name, gctf_data_path)

# https://github.com/graphframes/graphframes/blob/master/src/main/scala/org/graphframes/lib/AggregateMessages.scala
# https://issues.apache.org/jira/browse/SPARK-13346
def getCachedDataFrame(spark, df):
    rdd = df.rdd.cache()
    return spark.createDataFrame(rdd, df.schema)
