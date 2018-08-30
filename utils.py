import json
import operator
import os
import random
from pyspark.sql.functions import lit
gctf_data_path='hdfs://spark-master0-dsl05:9000/gctf_data' #'/home/sprk/shared/gctf_data'


def iter_indices_gen_data(all_tensors_config, cardinalities, tensor_name, fd, zero_based_indices, iter_index_values=[]):
    tensor_index_names = all_tensors_config[tensor_name]['indices']
    if len(iter_index_values) == len(tensor_index_names):
        for iiv_index, iiv in enumerate(iter_index_values):
            if iiv_index != 0:
                fd.write(',')
            if zero_based_indices:
                iiv_str = str( iiv + 1 )
            else:
                iiv_str = str( iiv )
            fd.write( iiv_str )
        fd.write( ',%.5f\n' %random.random() )
    else:
        iter_index_name = tensor_index_names[ len(iter_index_values) ]
        for index_val in range(cardinalities[iter_index_name]):
            iter_indices_gen_data( all_tensors_config, cardinalities, tensor_name, fd, zero_based_indices, iter_index_values+[index_val] )

def generate_random_tensor_data_local(all_tensors_config, cardinalities, tensor_name, zero_based_indices=False):
    # generate tensor data on local file

    local_filename=os.path.join('/tmp', tensor_name+'.csv')
    print(local_filename)
    print ('generate_random_tensor_data_local: generating %s' %local_filename)
    assert not os.path.exists(local_filename), 'data file %s exists can not procede' %local_filename

    fd = open(local_filename, 'w')

    # print header
    for index_index, index_name in enumerate(all_tensors_config[tensor_name]['indices']):
        if index_index != 0:
            fd.write(',')
        fd.write(index_name)
    fd.write(',value\n')

    iter_indices_gen_data(all_tensors_config, cardinalities, tensor_name, fd, zero_based_indices)
    fd.close()

    all_tensors_config[tensor_name]['local_filename'] = local_filename

def generate_local_tensor_data(all_tensors_config, cardinalities, tensor_name, indices, generate_local_data=False):
    assert tensor_name not in all_tensors_config, 'tensor_name %s already exists in gctf_model %s' %(tensor_name, gctf_model)

    all_tensors_config[tensor_name] = {
        'indices' : indices,
        'numel' : reduce(operator.mul, [cardinalities[i] for i in indices])
    }

    if generate_local_data:
        generate_random_tensor_data_local(all_tensors_config, cardinalities, tensor_name)

def get_all_indices(all_tensors_config):
    all_indices = set()
    for tensor_name, tensor in all_tensors_config.iteritems():
        all_indices.update( tensor['indices'] )
    # all_indices must have fixed ordering
    return list(sorted(all_indices))

def read_tensor_data_from_hdfs(spark_session, tensor_dataframe, all_tensors_config, tensor_name, root_path):
    tensor_def = all_tensors_config[tensor_name]
    hdfs_filename = tensor_def['hdfs_filename'] # TODO assert for hdfs file existance
    print( 'initializing data_local for %s using file %s' %(tensor_name, hdfs_filename) )

    schema = ''
    for index_index, index in enumerate(tensor_def['indices']):
        if index_index != 0:
            schema += ','
        schema += index + ' INT '
    schema += ', value DOUBLE'

    print('read_tensor_data: reading file %s with schema %s' %(hdfs_filename, schema))

    tensor_data = spark_session.read.load(hdfs_filename, format="csv", sep=",", header="true", schema=schema) #, inferSchema="true")
    #tensor_def['data_local'] = tensor_data.collect() # TODO: remove collect dependency (working on same tensors to produce output does not work with map function) -> use single rdd with tensor data annotated with tensor name

    # add missing columns
    missing_columns = set(get_all_indices(all_tensors_config))-set(tensor_def['indices'])
    for mc in missing_columns:
        tensor_data = tensor_data.withColumn(mc, lit(None))

    # add tensor name column
    tensor_data = tensor_data.withColumn('tensor_name', lit(tensor_name))

    if tensor_dataframe is None:
        print('read_tensor_data_from_hdfs: initializing tensor_dataframe')
        tensor_dataframe = tensor_data
    else:
        print('read_tensor_data_from_hdfs: update tensor_dataframe')
        tensor_dataframe = tensor_dataframe.unionByName(tensor_data)
    tensor_dataframe.cache()

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
        if obj  == operator.mul:
            return '*'
        elif obj == operator.truediv:
            return '/'
        elif obj == operator.add:
            return '+'

        return json.JSONEncoder.default(self, obj)
