import os.path

def read_tensor_data(spark_session, tensor_name, root_path, cache=True):
    filename='/'.join([root_path,tensor_name])+'.csv'
    assert os.path.isfile(filename), 'read_tensor_data: File %s does not exist, aborting'%filename
    tensor_data = spark_session.read.load(filename, format="csv", sep=",", inferSchema="true", header="true")
    if cache:
        tensor_data.cache()
    return tensor_data

def linear_index_to_tensor_index( all_tensors, all_cardinalities, linear_index, linear_index_tensor_name, tensor_index_tensor_name=None, as_dict=True ):
    current_stride = 1
    tensor_index_values_dict = {}
    if tensor_index_tensor_name is None:
        tensor_index_tensor_name = linear_index_tensor_name

    tensor_index_tensor_indices = all_tensors[tensor_index_tensor_name]['indices']
    for index_name in all_tensors[linear_index_tensor_name]['indices']:
        index_cardinality = all_cardinalities[index_name]
        value = ((linear_index / current_stride) % index_cardinality) + 1
        current_stride *= index_cardinality
        if index_name in tensor_index_tensor_indices:
            tensor_index_values_dict[index_name] = value

    if as_dict:
        return tensor_index_values_dict
    else:
        # make sure ordering is in 'tensor_index_tensor_name' indices order
        tensor_index_values_list = []
        for index_name in tensor_index_tensor_indices:
            tensor_index_values_list.append(tensor_index_values_dict[index_name])
        return tuple(tensor_index_values_list)
