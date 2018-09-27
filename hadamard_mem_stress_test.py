from hadamard import *
import sys

if __name__ == '__main__':
    tensors = {
        'gtp_test_input1' : {
            'indices' : [ 'i', 'k' ],
            'local_filename' : '/home/sprk/shared/gctf_data/gtp_test_input1.csv'
        },
        'gtp_test_output_hadamard' : {
            'indices' :  [ 'i', 'k' ],
            'local_filename' : '/home/sprk/shared/gctf_data/gtp_test_output_hadamard.csv',
        }
    }

    # put input local data onto hdfs
    local_files_str=''
    for tensor_name in tensors:
        if 'local_filename' in tensors[tensor_name]:
            local_files_str+=' ' + tensors[tensor_name]['local_filename']
    cmd = "$HADOOP_HOME/bin/hadoop fs -put %s %s" %(local_files_str, gctf_data_path)
    print( cmd )
    os.system( cmd )

    # load hdfs data into spark
    spark = SparkSession.builder.appName("gtp").getOrCreate()
    for tensor_name in tensors:
        if 'local_filename' in tensors[tensor_name]:
            tensors[tensor_name]['hdfs_filename'] = '/'.join([gctf_data_path, tensors[tensor_name]['local_filename'].split('/')[-1]])
            tensors[tensor_name]['df'] =  read_tensor_data_from_hdfs(spark, tensors, tensor_name, gctf_data_path)

    # TEST CASE: stress test, repeat test 5 200 times hadamard( DataFrame (pre_processor), DataFrame )
    # TODO: check memory usage before and after execution automatically

    print('hadamard_mem_stress_test: waiting for mem usage drop test')
    sys.stdout.flush()
    import time
    time.sleep(10)

    for i in range( 400 ):
        hadamard(spark, tensors, {
            'operation_type':'hadamard',
            'output': 'gtp_test_output_hadamard',
            'input':{
                'combination_operator':operator.mul, #input must be scalar or same size as output
                'arguments': [
                    {
                        'data':'gtp_test_input1',
                        'pre_processor':{
                            'operator':'pow',
                            'argument':-1
                        }
                    },
                    {
                        'data':'gtp_test_input1'
                    }
                ]
            }
        })
        
        for row in tensors['gtp_test_output_hadamard']['df'].collect():
            assert row['value'] == 1, 'wrong output %s' %str(row)

    print('hadamard_mem_stress_test: waiting for mem usage drop test')
    import time
    time.sleep(10000)
    print('hadamard_mem_stress_test done')

