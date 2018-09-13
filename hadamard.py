import operator
import functools
from utils import gctf_data_path
from utils import ComplexEncoder
import json
from pyspark.sql import DataFrame
from pyspark.sql import Column
import os
from pyspark.sql import SparkSession
from utils import read_tensor_data_from_hdfs
from pyspark.sql import functions as PysparkSQLFunctions
from pyspark.sql.functions import lit
from pyspark.sql.functions import array_contains
import math

def apply_pre_processor_helper(value, pre_processor_spec):
    if pre_processor_spec['operator'] == 'pow':
        # TODO: use pyspark.sql.functions.pow(col1, col2)
        return pow(value, pre_processor_spec['argument'])
    elif pre_processor_spec['operator'] == 'log':
        return PysparkSQLFunctions.log(value)
    else:
        raise Exception('unknown pre_processor %s' %pre_processor_spec)


def apply_pre_processor(de_prep):
    data_element = de_prep[0]
    if len(de_prep) == 1:
        # no pre_processor
        if isinstance(data_element, DataFrame):
            output_element = data_element.withColumnRenamed('value', 'output')
        elif isinstance(data_element, Column): #is_number(data_element):
            output_element = data_element
        else:
            raise Exception('unknown data element (2)')

    elif len(de_prep) == 2:
        # yes pre_processor
        pre_processor_spec = de_prep[1]
        if isinstance(data_element, DataFrame):
            output_element = data_element.withColumn('output', apply_pre_processor_helper(data_element['value'], pre_processor_spec))
        elif isinstance(data_element, Column): #is_number(data_element):
            output_element = apply_pre_processor_helper(data_element, pre_processor_spec)
        else:
            raise Exception('unknown data element (3)')

    else:
        raise Exception('hadamard: de_prep must have length 1 or 2')

    return output_element


# TODO: remove suboperation - redundant level
def process_operation(spark, all_tensors_config, input_spec, level=0, debug=False):
    # prepare output element (DataFrame or numeric) for all operands
    global process_operation_index_set # used for sanity checking on master node

    all_arguments = []
    for argument in input_spec['arguments']:
        if 'suboperation' in argument:
            all_arguments.append( (process_operation(spark, all_tensors_config, argument['suboperation'], level=level+1, debug=debug),) )
        else:
            # fetch data element
            assert 'data' in argument, 'process_operation: if argument is not a suboperation, it must have a data element, this argument does not have it, bad argument bad! %s' % json.dumps(argument, indent=4, sort_keys=True, cls=ComplexEncoder )
            data_element = argument['data'] # can be a string representing a dataframe, can be a scalar numeric value
            if isinstance(data_element, basestring):
                tensor_name = data_element
                assert tensor_name in all_tensors_config, 'tensor_name %s must be located in all_tensors_config %s to treat it as a tensor' %(tensor_name, json.dumps( all_tensors_config, indent=4, sort_keys=True, cls=ComplexEncoder ))
                data_element = all_tensors_config[tensor_name]['df']

                # sanity check
                if process_operation_index_set:
                    assert set(all_tensors_config[tensor_name]['indices']) == process_operation_index_set, 'hadamard: tensor operands must have same set of indices'
                process_operation_index_set = set(all_tensors_config[tensor_name]['indices'])

            elif isinstance(data_element, Column): # or is_number(data_element):
                data_element = data_element

            else:
                raise Exception('unknown data element (1)')

            # add data_element to all_arguments
            if 'pre_processor' in argument:
                all_arguments.append( (data_element, argument['pre_processor'] ) )

            elif 'data' in argument:
                all_arguments.append( (data_element, ) )

            else:
                raise Exception('unknown argument')


    # apply arithmetic operation with pre-processor and return (data_element, pre_processor) pair
    for de_prep_index, de_prep in enumerate(all_arguments):
        # TODO: need to check if level 0 should have more than 1 argument? NOT SURE?

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
                output_de = output_de.withColumn('final_output', input_spec['combination_operator'](output_de.output, pre_processed_de))

            else: # both numeric
                output_de = input_spec['combination_operator'](output_de, pre_processed_de)

    if isinstance(output_de, DataFrame):
        output_de = output_de.drop('output', 'value').withColumnRenamed('final_output', 'value')

    if debug:
        print('hadamard input_spec %s level %s output_de %s values %s' %(json.dumps( input_spec, indent=4, sort_keys=True, cls=ComplexEncoder ), level,output_de, output_de.collect()))

    return output_de

def hadamard(spark, all_tensors_config, update_rule, debug=False):
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
    output_tensor_name = update_rule['output']
    all_tensors_config[output_tensor_name]['df'] = process_operation(spark, all_tensors_config, update_rule['input'], debug=debug)


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


    # TEST CASE 1: hadamard( DataFrame, DataFrame )
    hadamard(spark, tensors, {
        'operation_type':'hadamard',
        'output': 'gtp_test_output_hadamard',
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
    })

    for row in tensors['gtp_test_output_hadamard']['df'].collect():
        if row.i == 1 and row.k == 1:
            assert row['value'] == 100, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 2:
            assert row['value'] == 900, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 3:
            assert row['value'] == 2500, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 4:
            assert row['value'] == 4900, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 1:
            assert row['value'] == 400, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 2:
            assert row['value'] == 1600, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 3:
            assert row['value'] == 3600, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 4:
            assert row['value'] == 6400, 'wrong output %s' %str(row)
        else:
            raise Exception('unexpected index values %s' %str(row))

    print('test case 1 done')

    # TEST CASE 2: hadamard( DataFrame, scalar number )
    hadamard(spark, tensors, {
        'operation_type':'hadamard',
        'output': 'gtp_test_output_hadamard',
        'input':{
            'combination_operator':operator.mul,
            'arguments': [
                {
                    'data':'gtp_test_input1'
                },
                {
                    'data':lit(2)
                }
            ]
        }
    })

    for row in tensors['gtp_test_output_hadamard']['df'].collect():
        if row.i == 1 and row.k == 1: # 10
            assert row['value'] == 20, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 2: # 30
            assert row['value'] == 60, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 3: # 50
            assert row['value'] == 100, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 4: # 70
            assert row['value'] == 140, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 1: # 20
            assert row['value'] == 40, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 2: # 40
            assert row['value'] == 80, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 3: # 60
            assert row['value'] == 120, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 4: # 80
            assert row['value'] == 160, 'wrong output %s' %str(row)
        else:
            raise Exception('unexpected index values %s' %str(row))

    print('test case 2 done')

    # TEST CASE 3: hadamard( scalar number, DataFrame )
    hadamard(spark, tensors, {
        'operation_type':'hadamard',
        'output': 'gtp_test_output_hadamard',
        'input':{
            'combination_operator':operator.mul,
            'arguments': [
                {
                    'data':lit(3)
                },
                {
                    'data':'gtp_test_input1'
                }
            ]
        }
    })

    for row in tensors['gtp_test_output_hadamard']['df'].collect():
        if row.i == 1 and row.k == 1: # 10
            assert row['value'] == 30, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 2: # 30
            assert row['value'] == 90, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 3: # 50
            assert row['value'] == 150, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 4: # 70
            assert row['value'] == 210, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 1: # 20
            assert row['value'] == 60, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 2: # 40
            assert row['value'] == 120, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 3: # 60
            assert row['value'] == 180, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 4: # 80
            assert row['value'] == 240, 'wrong output %s' %str(row)
        else:
            raise Exception('unexpected index values %s' %str(row))

    print('test case 3 done')

    # TEST CASE 4: hadamard( scalar number, number )
    hadamard(spark, tensors, {
        'operation_type':'hadamard',
        'output': 'gtp_test_output_hadamard',
        'input':{
            'combination_operator':operator.mul,
            'arguments': [
                {
                    'data':lit(3)
                },
                {
                    'data':lit(4)
                }
            ]
        }
    })

    assert int(eval(tensors['gtp_test_output_hadamard']['df']._jc.toString())) == 12, 'wrong output %s' %str(tensors['gtp_test_output_hadamard']['df'])
    print('test case 4 done')





    # TEST CASE 5: hadamard( DataFrame (pre_processor), DataFrame )
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

    print('test case 5 done')

    # TEST CASE 6: hadamard( DataFrame, DataFrame (pre_processor))
    hadamard(spark, tensors, {
        'operation_type':'hadamard',
        'output': 'gtp_test_output_hadamard',
        'input':{
            'combination_operator':operator.mul, #input must be scalar or same size as output
            'arguments': [
                {
                    'data':'gtp_test_input1'
                },
                {
                    'data':'gtp_test_input1',
                    'pre_processor':{
                        'operator':'pow',
                        'argument':-1
                    }
                }
            ]
        }
    })

    for row in tensors['gtp_test_output_hadamard']['df'].collect():
        assert row['value'] == 1, 'wrong output %s' %str(row)

    print('test case 6 done')

    # TEST CASE 7: hadamard( DataFrame(pre_processor), DataFrame(pre_processor) )
    hadamard(spark, tensors, {
        'operation_type':'hadamard',
        'output': 'gtp_test_output_hadamard',
        'input':{
            'combination_operator':operator.mul,
            'arguments': [
                {
                    'data':'gtp_test_input1',
                    'pre_processor':{
                        'operator':'pow',
                        'argument':-1
                    }
                },
                {
                    'data':'gtp_test_input1',
                    'pre_processor':{
                        'operator':'pow',
                        'argument':-1
                    }
                }
            ]
        }
    })

    for row in tensors['gtp_test_output_hadamard']['df'].collect():
        if row.i == 1 and row.k == 1: # 10
            assert abs(row['value'] - 1.0/100) < 0.0001, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 2: # 30
            assert abs(row['value'] - 1.0/900) < 0.0001, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 3: # 50
            assert abs(row['value'] - 1.0/2500) < 0.0001, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 4: # 70
            assert abs(row['value'] - 1.0/4900) < 0.0001, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 1: # 20
            assert abs(row['value'] - 1.0/400) < 0.0001, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 2: # 40
            assert abs(row['value'] - 1.0/1600) < 0.0001, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 3: # 60
            assert abs(row['value'] - 1.0/3600) < 0.0001, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 4: # 80
            assert abs(row['value'] - 1.0/6400) < 0.0001, 'wrong output %s' %str(row)
        else:
            raise Exception('unexpected index values %s' %str(row))

    print('test case 7 done')

    # TEST CASE 8: hadamard( DataFrame, scalar number (pre_processor) )
    hadamard(spark, tensors, {
        'operation_type':'hadamard',
        'output': 'gtp_test_output_hadamard',
        'input':{
            'combination_operator':operator.mul,
            'arguments': [
                {
                    'data':'gtp_test_input1'
                },
                {
                    'data':lit(2),
                    'pre_processor':{
                        'operator':'pow',
                        'argument':2
                    }
                }
            ]
        }
    })

    for row in tensors['gtp_test_output_hadamard']['df'].collect():
        if row.i == 1 and row.k == 1: # 10
            assert row['value'] == 40, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 2: # 30
            assert row['value'] == 120, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 3: # 50
            assert row['value'] == 200, 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 4: # 70
            assert row['value'] == 280, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 1: # 20
            assert row['value'] == 80, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 2: # 40
            assert row['value'] == 160, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 3: # 60
            assert row['value'] == 240, 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 4: # 80
            assert row['value'] == 320, 'wrong output %s' %str(row)
        else:
            raise Exception('unexpected index values %s' %str(row))

    print('test case 8 done')






    # TEST CASE 9: hadamard( DataFrame, suboperation(scalar number, DataFrame) )
    hadamard(spark, tensors, {
        'operation_type':'hadamard',
        'output': 'gtp_test_output_hadamard',
        'input':{
            'combination_operator':operator.mul, #input must be scalar or same size as output
            'arguments': [
                {
                    'data':'gtp_test_input1'
                },
                {
                    'suboperation':{
                        'combination_operator':operator.mul,
                        'arguments':[
                            {
                                'data':lit(3),
                                'pre_processor':{
                                    'operator':'log'
                                }
                            },
                            {
                                'data':'gtp_test_input1'
                            }
                        ]
                    }
                }
            ]
        }
    })

    def hef(v):
        return v*(math.log(3)*v)

    for row in tensors['gtp_test_output_hadamard']['df'].collect():
        if row.i == 1 and row.k == 1: # 10
            assert int(row['value']) == int(hef(10)), 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 2: # 30
            assert int(row['value']) == int(hef(30)), 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 3: # 50
            assert int(row['value']) == int(hef(50)), 'wrong output %s' %str(row)
        elif row.i == 1 and row.k == 4: # 70
            assert int(row['value']) == int(hef(70)), 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 1: # 20
            assert int(row['value']) == int(hef(20)), 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 2: # 40
            assert int(row['value']) == int(hef(40)), 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 3: # 60
            assert int(row['value']) == int(hef(60)), 'wrong output %s' %str(row)
        elif row.i == 2 and row.k == 4: # 80
            assert int(row['value']) == int(hef(80)), 'wrong output %s' %str(row)
        else:
            raise Exception('unexpected index values %s' %str(row))

    print('test case 9 done')

    print('all tests completed')
