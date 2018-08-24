import os
import random

def iter_indices_gen_data(tensors_config, cardinalities, tensor_name, fd, zero_based_indices, iter_index_values=[]):
    tensor_index_names = tensors_config[tensor_name]['indices']
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
            iter_indices_gen_data( tensors_config, cardinalities, tensor_name, fd, zero_based_indices, iter_index_values+[index_val] )

def generate_random_tensor_data(tensors_config, cardinalities, tensor_name, hdfs_data_path, zero_based_indices=False):
    hdfs_filename='/'.join([hdfs_data_path, tensor_name+'.csv'])
    tmp_filename=os.path.join('/tmp', tensor_name+'.csv')
    print(tmp_filename)
    print ('generate_random_tensor_data: generating %s' %hdfs_filename)
    assert not os.path.exists(tmp_filename), 'data file %s exists can not procede' %tmp_filename
    # TODO: add assert with hdfs_filename

    fd = open(tmp_filename, 'w')

    # print header
    for index_index, index_name in enumerate(tensors_config[tensor_name]['indices']):
        if index_index != 0:
            fd.write(',')
        fd.write(index_name)
    fd.write(',value\n')

    iter_indices_gen_data(tensors_config, cardinalities, tensor_name, fd, zero_based_indices)
    fd.close()

    cmd = "$HADOOP_HOME/bin/hadoop fs -put %s %s" %(tmp_filename, hdfs_filename)
    print(cmd)
    os.system(cmd)

if __name__ == '__main__':
    from tests import gctf_model
    for tensor_name in tensors_config:
        generate_random_tensor_data(gctf_model['tensors'], gctf_model['config']['cardinalities'], tensor_name, '/tmp', zero_based_indices=True)
