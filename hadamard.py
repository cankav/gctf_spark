import operator
import functools
from utils import gctf_data_path
from utils import ComplexEncoder
import json
from utils import is_number
from pyspark.sql import DataFrame
import os
from pyspark.sql import SparkSession
from utils import read_tensor_data_from_hdfs

def apply_pre_processor_helper(value, pre_processor_spec):
    if pre_processor_spec['operator'] == 'pow':
        return pow(value, pre_processor_spec['argument'])
    else:
        raise Exception('unknown pre_processor %s' %pre_processor_spec)


def apply_pre_processor(de_prep):
    data_element = de_prep[0]
    if len(de_prep) == 1:
        # no pre_processor
        if isinstance(data_element, DataFrame):
            output_element = data_element.withColumnRenamed('value', 'output')
        elif is_number(data_element):
            output_element = data_element
        else:
            raise Exception('unknown data element (2)')

    elif len(de_prep) == 2:
        # yes pre_processor
        pre_processor_spec = de_prep[1]
        if isinstance(data_element, DataFrame):
            output_element = data_element.withColumn('output', apply_pre_processor_helper(data_element.value, pre_processor_spec))
        elif is_number(data_element):
            output_element = apply_pre_processor_helper(data_element, pre_processor_spec)
        else:
            raise Exception('unknown data element (3)')

    else:
        raise Exception('hadamard: de_prep must have length 1 or 2')

    return output_element


def process_operation(spark, all_tensors_config, input_spec, level=0):
    # prepare output element (DataFrame or numeric) for all operands
    global process_operation_index_set # used for sanity checking on master node

    all_arguments = []
    for argument in input_spec['arguments']:
        if 'suboperation' in argument:
            all_arguments.append( process_operation( process_operation(spark, all_tensors_config, argument, level+1) ) )
        else:
            # fetch data element
            data_element = argument['data'] # can be a string representing a dataframe, can be a scalar numeric value
            if isinstance(data_element, basestring):
                data_element = all_tensors_config[tensor_name]['df']

                # sanity check
                if process_operation_index_set:
                    assert set(all_tensors_config[tensor_name]['indices']) == process_operation_index_set, 'hadamard: tensor operands must have same set of indices'
                process_operation_index_set = set(all_tensors_config[tensor_name]['indices'])

            elif is_number(data_element):
                data_element = data_element

            else:
                raise Exception('unknown data element (1)')

            if 'pre_processor' in argument:
                all_arguments.append( (data_element, argument['pre_processor'] ) )

            elif 'data' in argument:
                all_arguments.append( (data_element, ) )

            else:
                raise Exception('unknown argument')


    # apply arithmetic operation with pre-processor and return (data_element, pre_processor) pair
    for de_prep_index, de_prep in enumerate(all_arguments):
        pre_processed_de = apply_pre_processor(de_prep) # output_element may be a DataFrame or scalar numeric

        if de_prep_index == 0:
            output_de = pre_processed_de

        else:
            # got 2 elements to merge with input_spec['combination_operator']: output_de and pre_processed_de, each one may be a DataFrame or scalar numeric
            if isinstance(output_de, DataFrame) and isinstance(pre_processed_de, DataFrame):
                output_de = output_de.join(
                    pre_processed_de,
                    list(process_operation_index_set)
                ).withColumn( 'final_output',
                              input_spec['combination_operator'](output_de.output, pre_processed_de.output) )

            elif not isinstance(output_de, DataFrame) and isinstance(pre_processed_de, DataFrame):
                output_de = pre_processed_de.withColumn('final_output', input_spec['combination_operator'](output_de, pre_processed_de.output))

            elif isinstance(output_de, DataFrame) and not isinstance(pre_processed_de, DataFrame):
                output_de = output_de.withColumn('final_output', input_spec['combination_operator'](output_de.value, pre_processed_de))

            else: # both numeric
                output_de = input_spec['combination_operator'](output_de, pre_processed_de)

    if isinstance(output_de, DataFrame):
        output_de = output_de.drop('output').withColumnRenamed('final_output', 'output')

    return output_de

def hadamard(spark, all_tensors_config, update_rule):
    # 'input': {
    #   'combination_operator': < built - in function add > ,
    #   'arguments': [{
    #     'data': '_gtp_d1_alpha_Z1'
    #   }, {
    #     'suboperation': {
    #       'combination_operator': < built - in function mul > ,
    #       'arguments': [{
    #         'data': 1,
    #         'pre_processor': {
    #           'operator': 'pow',
    #           'argument': -1
    #         }
    #       }, {
    #         'data': '_gtp_d1_delta_Z1'
    #       }]
    #     }
    #   }]
    # }

    # 'input': {
    #   'combination_operator': < built - in function add > ,
    #   'arguments': [{
    #     'data': '_gtp_d1_alpha_Z1'
    #   }, {
    #     'suboperation': {
    #       'combination_operator': < built - in function mul > ,
    #       'arguments': [{
    #         'data': 1,
    #         'pre_processor': {
    #           'operator': 'pow',
    #           'argument': -1
    #         }
    #       }, {
    #         'data': '_gtp_d1_delta_Z1'
    #       }]
    #     }
    #   }]
    # }

    assert update_rule['operation_type'] == 'hadamard', 'hadamard can only work with update_rules with hadamard operation_type but found update_rule %s' %json.dumps( update_rule, indent=4, sort_keys=True, cls=ComplexEncoder )

    print('EXECUTING RULE: starting hadamard operation update_rule %s' %json.dumps( update_rule, indent=4, sort_keys=True, cls=ComplexEncoder ))

    global process_operation_index_set
    process_operation_index_set=None
    return process_operation(spark, all_tensors_config, update_rule['input'])
    

if __name__ == '__main__':
    tensors = {
        'gtp_test_input1' : {
            'indices' : [ 'i', 'k' ],
            'local_filename' : '/home/sprk/shared/gctf_data/gtp_test_input1.csv'
        },
        'gtp_test_input2' : {
            'indices' :  [ 'j', 'k' ],
            'local_filename' : '/home/sprk/shared/gctf_data/gtp_test_input2.csv',
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


    # TEST CASE: hadamard( DataFrame, DataFrame )
    hadamard(spark, tensors, {
        'operation_type':'hadamard',
        'input':{
            'combination_operator':operator.mul, #input must be scalar or same size as output
            'arguments': [
                {
                    'data':'gtp_test_input1'
                },
                {
                    'data':'gtp_test_input1'
                }
            ]
        }
    }).show(n=1000)

