from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from operator import add
from utils import read_tensor_data_from_hdfs
from utils import linear_index_to_tensor_index
from utils import gctf_data_path
import os
from utils import ComplexEncoder
import json

def find_tensor_value(gtp_spec, input_tensor_name, tensor_index_values):
    tensor_spec = gtp_spec['tensors'][input_tensor_name]
    tensor_data = tensor_spec['data_local']

    #for tiv in tensor_index_values:
    #    tensor_data.filter(str(tiv) + '=' + str(tensor_index_values[tiv]))
    #return tensor_data

    for row in tensor_data:
        matched_index_count = 0
        for tensor_index_name in tensor_spec['indices']:
            if row[tensor_index_name] == tensor_index_values[tensor_index_name]:
                matched_index_count += 1
        if matched_index_count == len(tensor_spec['indices']):
            print('was %s' %str(row))
            return row['value']
    return None

def gtp(spark, gtp_spec, gctf_model=None):
    print('EXECUTING RULE: starting GTP operation gtp_spec %s' %json.dumps( gtp_spec, indent=4, sort_keys=True, cls=ComplexEncoder ))

    # get all indices
    full_tensor_name = '_gtp_full_tensor'
    if full_tensor_name not in gtp_spec['tensors']:
        all_indices = set()
        for tensor_name, tensor in gtp_spec['tensors'].iteritems():
            all_indices.update( tensor['indices'] )
        # all_indices must have fixed ordering
        all_indices = list(all_indices)

        # add full tensor to the tensor list
        gtp_spec['tensors']['_gtp_full_tensor'] = {'indices':all_indices, 'tags':['do_not_initialize_from_disk',]}

        # indices are assumed to be serialized left to right
        for tensor_name, tensor in gtp_spec['tensors'].iteritems():
            tensor['strides'] = []
            current_stride = 1
            tensor['numel'] = 1
            for tensor_index_name in tensor['indices']:
                tensor['strides'].append(current_stride)
                current_stride *= gtp_spec['config']['cardinalities'][tensor_index_name]
                tensor['numel'] *= gtp_spec['config']['cardinalities'][tensor_index_name]

        print ('gtp_spec %s' % gtp_spec)

    # map function to calculate each entry of the full tensor and compute one or more results output tensor indices
    def mapfunc(full_tensor_linear_index):
        print( '\n\nf %s' %full_tensor_linear_index)
        # calculate dimension indices from linear index,
        full_tensor_index_values = linear_index_to_tensor_index( gtp_spec['tensors'], gtp_spec['config']['cardinalities'], full_tensor_linear_index, full_tensor_name)

        # fetch corresponding input tensor values
        # multiply them and return (linear index, output_value)
        output_value = None
        for input_tensor_name in gtp_spec['config']['input']:
            input_tensor_value = find_tensor_value(gtp_spec, input_tensor_name, full_tensor_index_values) #spark.sql(query)
            if input_tensor_value is not None:
                if output_value is None:
                    output_value = input_tensor_value
                else:
                    output_value *= input_tensor_value
        print( 'full_tensor_index_values %s' %full_tensor_index_values )
        print( 'output_value %s' %output_value )
        if output_value is None:
            return None
        else:
            # convert full_tensor_linear_index to output full index
            output_tensor_index = linear_index_to_tensor_index( gtp_spec['tensors'], gtp_spec['config']['cardinalities'], full_tensor_linear_index, full_tensor_name, gtp_spec['config']['output'], as_dict=False )
            return (output_tensor_index, output_value)

    sc = spark.sparkContext
    rdd = sc.parallelize(xrange(gtp_spec['tensors']['_gtp_full_tensor']['numel']))
    rdd = rdd.map(mapfunc).reduceByKey(add)

    # reformat structure ( ((i,j,k), value), ... ) -> ( (i,j,k,value), ... )
    def build_row(indices_value_tuple):
        row = [indices_value_tuple[1]]
        for i in indices_value_tuple[0]:
            row.insert(0,i)
        return list(row)

    output_tensor_name = gtp_spec['config']['output']
    rdd = rdd.map( lambda x: build_row(x), gtp_spec['tensors'][output_tensor_name]['indices'])

    df = spark.createDataFrame(rdd) -- MUST ADD SCHEMA HERE
    if 'filename' in gtp_spec['tensors'][output_tensor_name]:
        filename = gtp_spec['tensors'][output_tensor_name]['filename']
    else:
        filename = '/'.join([gctf_data_path,output_tensor_name])+'.csv'
        gtp_spec['tensors'][output_tensor_name]['filename'] = filename

    print('df.write.csv filename %s df shape %s %s' %(filename, str(df.count()), str(len(df.columns))))
    df.write.csv(filename, mode='overwrite')

    # TODO: remove this with tensor name annotated data storage version
    if 'data_local' in gctf_model['tensors'][output_tensor_name]:
        print( 'gtp: tensor %s data before update %s' %(output_tensor_name, gctf_model['tensors'][output_tensor_name]['data_local']) )
    else:
        print( 'gtp: tensor %s data before update %s' %(output_tensor_name, None) )
    gctf_model['tensors'][output_tensor_name]['data_local'] = df.collect()
    print( 'gtp: tensor %s data after update %s' %(output_tensor_name, gctf_model['tensors'][output_tensor_name]['data_local']) )

    return (df, filename)

if __name__ == '__main__':
    gtp_spec = {
        'config': {
            'cardinalities' : {
                'i': 2,
                'j': 3,
                'k': 4
            },
            'output' : 'gtp_test_output',
            'input' : [ 'gtp_test_input1', 'gtp_test_input2' ]
        },
        'tensors' : {
            'gtp_test_output' : {
                'indices' : [ 'i', 'j' ],
                'local_filename' : '/home/sprk/shared/gctf_data/gtp_test_output.csv',
                'tags' : ['do_not_initialize_from_disk',]
            },
            'gtp_test_input1' : {
                'indices' : [ 'i', 'k' ],
                'local_filename' : '/home/sprk/shared/gctf_data/gtp_test_input1.csv',
                'tags' : []
            },
            'gtp_test_input2' : {
                'indices' :  [ 'j', 'k' ],
                'local_filename' : '/home/sprk/shared/gctf_data/gtp_test_input2.csv',
                'tags' : []
            }
        }
    }

    spark = SparkSession.builder.appName("gtp").getOrCreate()

    # put local data onto hdfs
    local_files_str=''
    for tensor_name in gtp_spec['tensors']:
        if 'do_not_initialize_from_disk' not in gtp_spec['tensors'][tensor_name]['tags']:
            local_files_str+=' ' + gtp_spec['tensors'][tensor_name]['local_filename']
    cmd = "$HADOOP_HOME/bin/hadoop fs -put %s %s" %(local_files_str, gctf_data_path)
    print( cmd )
    os.system( cmd )
    for tensor_name in gtp_spec['tensors']:
        if 'do_not_initialize_from_disk' in gtp_spec['tensors'][tensor_name]['tags']:
            filename = tensor_name
        else:
            filename = gtp_spec['tensors'][tensor_name]['local_filename'].split('/')[-1]

        gtp_spec['tensors'][tensor_name]['hdfs_filename'] = '/'.join([gctf_data_path, filename])

    # put hdfs data into spark memory
    for tensor_name in gtp_spec['config']['input']:
        assert 'data_local' not in gtp_spec['tensors'][tensor_name], 'gtp: Tensor %s should not have dataframe loaded at this point' %tensor_name
        if 'do_not_initialize_from_disk' not in gtp_spec['tensors'][tensor_name]['tags']:
            read_tensor_data_from_hdfs(spark, tensor_name, gctf_data_path, gtp_spec['tensors'][tensor_name])

    [df, output_filename] = gtp(spark, gtp_spec)

    # TODO: automatic check of the matrix product

    spark.stop()

# >> one*two
#ans =        11800       13400       15000
#             14000       16000       18000
# i, j, value
# 1, 1, 118000
# 2, 1, 140000
