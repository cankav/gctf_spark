from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from operator import add
from utils import read_tensor_data_from_hdfs
from utils import linear_index_to_DOK_index
from utils import gctf_data_path
import os
from utils import ComplexEncoder
import json
from utils import get_all_indices
from utils import generate_local_tensor_data

def gtp(spark, gtp_spec, tensor_dataframe, gctf_model=None):
    print('EXECUTING RULE: starting GTP operation gtp_spec %s' %json.dumps( gtp_spec, indent=4, sort_keys=True, cls=ComplexEncoder ))

    full_tensor_name='gtp_full_tensor'
    if full_tensor_name not in gtp_spec['tensors']:
        print('creating full tensor for gtp')

        cards=gtp_spec['config']['cardinalities']
        # TODO: replace with streaming data generation instead of file transfer to remove file size limitation
        generate_local_tensor_data(gtp_spec['tensors'], cards, full_tensor_name, cards.keys(), generate_local_data=True)
        cmd = "$HADOOP_HOME/bin/hadoop fs -put %s %s" %(gtp_spec['tensors'][full_tensor_name]['local_filename'], gctf_data_path)
        print( cmd )
        os.system( cmd )
        # load hdfs data into spark
        gtp_spec['tensors'][full_tensor_name]['hdfs_filename'] = '/'.join([gctf_data_path, full_tensor_name+'.csv'])
        tensor_dataframe = read_tensor_data_from_hdfs(spark, tensor_dataframe, gtp_spec['tensors'], full_tensor_name, gctf_data_path)
    else:
        # clear full tensor elements not necessary because all entries will be re-calculated
        print('full tensor for gtp already created')
        pass

    # map function to calculate each entry of the full tensor and compute one or more results output tensor indices
    def compute_full_tensor(tensor_dataframe_row):
        if tensor_dataframe_row['tensor_name'] == full_tensor_name:
            # find input elements for this row of full tensor entry
            new_f_tensor_value = 1
            for tensor_name in gtp_spec['config']['input']:
                input_value = 1 # tensor_dataframe.filter(tensor_name=tensor_name)
                for index_name in gtp_spec['config']['cardinalities']:
                    input_value = 3 # input_value.filter(index_name==tensor_dataframe_row[index_name] | index_name)
                #assert len(input_value) == 1, 'each f tensor entry must correspond to a distinct row for each input tensor'
                new_f_tensor_value *= input_value #[0]['value']
            print('old value %s new value %s' %(tensor_dataframe_row.value, new_f_tensor_value))
            tensor_dataframe_row.value = new_f_tensor_value
            
            return tensor_dataframe_row
        else:
            return tensor_dataframe_row

    tensor_dataframe = tensor_dataframe.rdd.map(compute_full_tensor) #.map(compute_output_tensor)

    return tensor_dataframe

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
                'indices' : [ 'i', 'j' ]
            },
            'gtp_test_input1' : {
                'indices' : [ 'i', 'k' ],
                'local_filename' : '/home/sprk/shared/gctf_data/gtp_test_input1.csv'
            },
            'gtp_test_input2' : {
                'indices' :  [ 'j', 'k' ],
                'local_filename' : '/home/sprk/shared/gctf_data/gtp_test_input2.csv',
            }
        }
    }

    spark = SparkSession.builder.appName("gtp").getOrCreate()

    # put input local data onto hdfs
    local_files_str=''
    for tensor_name in gtp_spec['tensors']:
        if 'local_filename' in gtp_spec['tensors'][tensor_name]:
            local_files_str+=' ' + gtp_spec['tensors'][tensor_name]['local_filename']
    cmd = "$HADOOP_HOME/bin/hadoop fs -put %s %s" %(local_files_str, gctf_data_path)
    print( cmd )
    os.system( cmd )

    # load hdfs data into spark
    tensor_dataframe = None # TODO: what if no input data is loaded?
    for tensor_name in gtp_spec['config']['input']:
        if 'local_filename' in gtp_spec['tensors'][tensor_name]:
            gtp_spec['tensors'][tensor_name]['hdfs_filename'] = '/'.join([gctf_data_path, gtp_spec['tensors'][tensor_name]['local_filename'].split('/')[-1]])
            tensor_dataframe = read_tensor_data_from_hdfs(spark, tensor_dataframe, gtp_spec['tensors'], tensor_name, gctf_data_path)

    tensor_dataframe = gtp(spark, gtp_spec, tensor_dataframe)

    for row in tensor_dataframe.collect():
        print row

    # TODO: automatic check of the matrix product

    spark.stop()

# >> one*two
#ans =        11800       13400       15000
#             14000       16000       18000
# i, j, value
# 1, 1, 118000
# 2, 1, 140000
