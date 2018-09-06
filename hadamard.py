from utils import linear_index_to_tensor_index
import operator
import functools
from utils import gctf_data_path
from utils import ComplexEncoder
import json

def process_operation(spark, all_tensors_config, input_spec):
    arguments_df = []
    for argument in input_spec['arguments']:
        if 'suboperation' in argument:
            arguments_df.append( process_operation(spark, gctf_model, argument) )
        if 'pre_processor' in argument:
            
            

def hadamard(spark, gctf_model, update_rule):
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
  
    print('EXECUTING RULE: starting hadamard operation update_rule %s' %json.dumps( update_rule, indent=4, sort_keys=True, cls=ComplexEncoder ))

    return process_operation(spark, gctf_model, update_rule['input'])
    


if __name__ == '__main__':
    # create DataFrame objects for tensors

    # execute hadamard operation

    # test output


    gctf_model = {
        'config' : {
            'cardinalities' : {
                'i' : 2,
                'j' : 3,
                'k' : 4,
                'r' : 5
            },
            'factorizations' : [
                {
                    'observed_tensor' : 'gctf_test_X1',
                    'latent_tensors' : [ 'gctf_test_Z1', 'gctf_test_Z2' ],
                    'p' : 1,
                    'phi' : 1
                },
                {
                    'observed_tensor' : 'gctf_test_X2',
                    'latent_tensors' : [ 'gctf_test_Z1', 'gctf_test_Z3' ],
                    'p' : 1,
                    'phi' : 1
                }
            ]
        },
        'tensors' : {
            'gctf_test_X1' : {
                'indices' : [ 'i', 'j' ]
            },
            'gctf_test_X2' : {
                'indices' : [ 'i', 'r' ]
            },
            'gctf_test_Z1' : {
                'indices' : [ 'i', 'k' ]
            },
            'gctf_test_Z2' : {
                'indices' : [ 'k', 'j' ]
            },
            'gctf_test_Z3' : {
                'indices' : [ 'k', 'r' ]
            }
        }
    }
