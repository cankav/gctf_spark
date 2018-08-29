import os.path
import json
import operator

gctf_data_path='hdfs://spark-master0-dsl05:9000/gctf_data' #'/home/sprk/shared/gctf_data'

def get_all_indices(tensor_config):
    all_indices = set()
    for tensor_name, tensor in tensor_config.iteritems():
        all_indices.update( tensor['indices'] )
    # all_indices must have fixed ordering
    return list(sorted(all_indices))

def read_tensor_data_from_hdfs(spark_session, tensor_dataframe, tensor_config, tensor_name, root_path):
    print( 'initializing data_local for %s using file %s as %s' %(tensor_name, hdfs_filename, tensor_def['data_local']) )

    if tensor_dataframe is None:
        print('read_tensor_data_from_hdfs: initializing tensor_dataframe as emptyRDD')
        tensor_dataframe = spark_session.emptyRDD()

    tensor_def = tensor_config[tensor_name]
    hdfs_filename = tensor_def['hdfs_filename'] # TODO assert for hdfs file existance

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
    missing_columns = set(get_all_indices(tensor_config))-set(tensor_def['indices'])
    for mc in missing_columns:
        tensor_data = tensor_data.withColumn(mc, None)

    # add tensor name column
    tensor_data = tensor_data.withColumn('tensor_name', tensor_name)

    tensor_dataframe = tensor_dataframe.unionByName(tensor_data)
    tensor_dataframe.cache()


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
