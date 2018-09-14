#from gtp import gtp
import operator
import copy
from pyspark.sql import SparkSession
from gtp import gtp
from utils import read_tensor_data_from_hdfs
from utils import gctf_data_path
from hadamard import hadamard
from generate_random_tensor_data import generate_random_tensor_data_local
import os
import json
from utils import ComplexEncoder
from utils import get_observed_tensor_names_of_latent_tensor
from utils import gengtp
from utils import create_full_tensor
from utils import full_tensor_name
from utils import generate_hdfs_tensor_data
from utils import generate_spark_tensor
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

def update_d1_Q_v(gctf_model, update_rules, observed_tensor_name, observed_tensor_xhat_name):
    update_rules.append( {
        'operation_type':'hadamard',
        'output':'gtp_d1_Q_v_'+observed_tensor_name,
        'input':{
            'combination_operator':operator.mul,
            'arguments':[
                {
                    'data':observed_tensor_name
                },
                {
                    'data':observed_tensor_xhat_name,
                    'pre_processor': {
                        'operator':'pow',
                        'argument':-1
                    }
                }
            ]
        }
    } )


def update_d1_delta(gctf_model, update_rules, latent_tensor_names, ltn, observed_tensor_name, other_Z_alpha_tensors):
    update_rules.append( {
        'operation_type':'gtp',
        'gtp_spec':gengtp(gctf_model, 'gtp_d1_delta_'+ltn, ['gtp_d1_Q_v_'+observed_tensor_name] + other_Z_alpha_tensors, full_tensor_name)
    } )


def update_d2_Q_v(update_rules, observed_tensor_name, observed_tensor_xhat_name, factorization):
    update_rules.append( {
        'operation_type':'hadamard',
        'output':'gtp_d2_Q_v_'+observed_tensor_name,
        'input':{
            'combination_operator':None,
            'arguments':[
                {
                    'data':observed_tensor_xhat_name,
                    'pre_processor':{
                        'operator':'pow',
                        'argument':1-factorization['p']
                    }
                }
            ]
        }
    } )


def update_d2_delta(gctf_model, update_rules, ltn, observed_tensor_name, other_Z_alpha_tensors):
    update_rules.append( {
        'operation_type':'gtp',
        'gtp_spec':gengtp(gctf_model, 'gtp_d2_delta_'+ltn, ['gtp_d2_Q_v_'+observed_tensor_name] + other_Z_alpha_tensors, full_tensor_name)
    } )


def update_d2_alpha(update_rules, ltn, factorization_index, factorization):
    if factorization_index == 0:
        update_rules.append( {
            #{end+1} = { '=', obj.d2_alpha(alpha), ['obj.config.tfmodel.phi_vector(' num2str(v_index) ')^-1 .* obj.config.tfmodel.d2_delta(' num2str(alpha) ').data'] };
            'operation_type':'hadamard',
            'output':'gtp_d2_alpha_'+ltn+'_v_'+factorization['observed_tensor'],
            'input':{
                'combination_operator':operator.mul, #input must be scalar or same size as output
                'arguments':[
                    {
                        'data':lit(factorization['phi']),
                        'pre_processor':{
                            'operator':'pow',
                            'argument':-1
                        }
                    },
                    {
                        'data':'gtp_d2_delta_'+ltn
                    }
                ]
            }
        } )

    else:
        update_rules.append( {
            #{end+1} = { '=', obj.d2_alpha(alpha), ['obj.config.tfmodel.d2_alpha(' num2str(alpha) ').data + obj.config.tfmodel.phi_vector(' num2str(v_index) ')^-1 .* obj.config.tfmodel.d2_delta(' num2str(alpha) ').data'] };
            'operation_type':'hadamard',
            'output':'gtp_d2_alpha_'+ltn+'_v_'+factorization['observed_tensor'],
            'input':{
                'combination_operator':operator.add,
                'arguments':[
                    {
                        'data':'gtp_d2_alpha_'+ltn+'_v_'+factorization['observed_tensor'],
                    },
                    {
                        'suboperation':{ # this input element is calculated using an expression
                            'combination_operator':operator.mul,
                            'arguments':[
                                {
                                    'data':lit(factorization['phi']),
                                    'pre_processor':{
                                        'operator':'pow',
                                        'argument':-1
                                    }
                                },
                                {
                                    'data':'gtp_d2_delta_'+ltn
                                }
                            ]
                        }
                    }
                ]
            }
        } )


def update_d1_alpha(gctf_model, update_rules, factorization_index, ltn, factorization):
    if factorization_index == 0:
        update_rules.append( {
            'operation_type':'hadamard',
            'output':'gtp_d1_alpha_'+ltn+'_v_'+factorization['observed_tensor'],
            'input':{
                'combination_operator':operator.mul, #input must be scalar or same size as output
                'arguments':[
                    {
                        'data':lit(factorization['phi']),
                        'pre_processor':{
                            'operator':'pow',
                            'argument':-1
                        }
                    },
                    {
                        'data':'gtp_d1_delta_'+ltn
                    }
                ]
            }
        } )

    else:
        update_rules.append( {
            'operation_type':'hadamard',
            'output':'gtp_d1_alpha_'+ltn+'_v_'+factorization['observed_tensor'],
            'input':{
                'combination_operator':operator.add,
                'arguments':[
                    {
                        'data':'gtp_d1_alpha_'+ltn+'_v_'+factorization['observed_tensor'],
                    },
                    {
                        'suboperation':{ # this input element is calculated using an expression
                            #obj.config.tfmodel.phi_vector(' num2str(v_index) ')^-1 .* obj.config.tfmodel.d1_delta(' num2str(alpha) ').data'
                            'combination_operator':operator.mul,
                            'arguments':[
                                {
                                    'data':lit(factorization['phi']),
                                    'pre_processor':{
                                        'operator':'pow',
                                        'argument':-1
                                    }
                                },
                                {
                                    'data':'gtp_d1_delta_'+ltn
                                }
                            ]
                        }
                    }
                ]
            }
        } )


def update_xhat(gctf_model, update_rules, observed_tensor_xhat_name, factorization):
    update_rules.append( {
        'operation_type':'gtp',
        'gtp_spec' : gengtp(gctf_model, observed_tensor_xhat_name, factorization['latent_tensors'], full_tensor_name)
    } )


def update_Z_alpha(gctf_model, update_rules, ltn):
    # { '=', obj.Z_alpha(alpha), ['obj.config.tfmodel.Z_alpha(' num2str(alpha) ').data .* obj.config.tfmodel.d1_alpha(' num2str(alpha) ').data ./ obj.config.tfmodel.d2_alpha('  num2str(alpha) ').data'] };
    rule = {
        'operation_type':'hadamard',
        'output':ltn,
        'input': {
            'combination_operator':operator.mul,
            'arguments' : [
                {
                    'data':ltn
                },
                {
                    'suboperation':{
                        'combination_operator':operator.truediv,
                        'arguments':None
                    }
                }
            ]
        }
    }

    otn_with_ltn=get_observed_tensor_names_of_latent_tensor(gctf_model, ltn)
    if len(otn_with_ltn) == 1:
        rule['input']['arguments'][1]['suboperation']['arguments'] = [
            {'data':'gtp_d1_alpha_'+ltn+'_v_'+otn_with_ltn[0]},
            {'data':'gtp_d2_alpha_'+ltn+'_v_'+otn_with_ltn[0]}
        ]
    else:
        rule['input']['arguments'][1]['suboperation']['arguments'] = [
            {
                'suboperation':{
                    'combination_operator':operator.add,
                    'arguments':[]
                }
            },
            {
                'suboperation':{
                    'combination_operator':operator.add,
                    'arguments':[]
                }
            }
        ]
        for otn in otn_with_ltn:
            rule['input']['arguments'][1]['suboperation']['arguments'][0]['suboperation']['arguments'].append(
                {'data':'gtp_d1_alpha_'+ltn+'_v_'+otn},
            )

            rule['input']['arguments'][1]['suboperation']['arguments'][1]['suboperation']['arguments'].append(
                {'data':'gtp_d2_alpha_'+ltn+'_v_'+otn},
            )

        update_rules.append(rule)


def gen_update_rules(spark, gctf_model):
    if full_tensor_name not in gctf_model['tensors']:
        create_full_tensor(spark, gctf_model['tensors'], gctf_model['config']['cardinalities'])

    # create intermediate tensors
    latent_tensor_names = set()
    for factorization in gctf_model['config']['factorizations']:
        observed_tensor_name = factorization['observed_tensor']
        generate_spark_tensor(spark, gctf_model['tensors'], gctf_model['config']['cardinalities'], 'gtp_hat_'+observed_tensor_name, gctf_model['tensors'][observed_tensor_name]['indices'])

        generate_spark_tensor(spark, gctf_model['tensors'], gctf_model['config']['cardinalities'], 'gtp_d1_Q_v_'+observed_tensor_name, gctf_model['tensors'][observed_tensor_name]['indices'])
        generate_spark_tensor(spark, gctf_model['tensors'], gctf_model['config']['cardinalities'], 'gtp_d2_Q_v_'+observed_tensor_name, gctf_model['tensors'][observed_tensor_name]['indices'])

        for ltn in factorization['latent_tensors']:
            latent_tensor_names.add( ltn )

            generate_spark_tensor(spark, gctf_model['tensors'], gctf_model['config']['cardinalities'], 'gtp_d1_alpha_'+ltn+'_v_'+observed_tensor_name, gctf_model['tensors'][ltn]['indices'])
            generate_spark_tensor(spark, gctf_model['tensors'], gctf_model['config']['cardinalities'], 'gtp_d2_alpha_'+ltn+'_v_'+observed_tensor_name, gctf_model['tensors'][ltn]['indices'])


    latent_tensor_names = list(latent_tensor_names)
    for ltn in latent_tensor_names:
        generate_spark_tensor(spark, gctf_model['tensors'], gctf_model['config']['cardinalities'], 'gtp_d1_delta_'+ltn, gctf_model['tensors'][ltn]['indices'])
        generate_spark_tensor(spark, gctf_model['tensors'], gctf_model['config']['cardinalities'], 'gtp_d2_delta_'+ltn, gctf_model['tensors'][ltn]['indices'])

    # get all indices
    all_indices = set()
    for tensor_name, tensor in gctf_model['tensors'].iteritems():
        all_indices.update( tensor['indices'] )
    # all_indices must have fixed ordering, NOT REALLY! NOT ANY MORE
    all_indices = list(all_indices)

    # indices are assumed to be serialized left to right, NOT ANY MORE
    for tensor_name, tensor in gctf_model['tensors'].iteritems():
        tensor['numel'] = 1
        for tensor_index_name in tensor['indices']:
            tensor['numel'] *= gctf_model['config']['cardinalities'][tensor_index_name]

    update_rules = []
    # update each Z_alpha
    for ltn in latent_tensor_names:
        # update each X_hat
        for factorization_index, factorization in enumerate(gctf_model['config']['factorizations']):
            observed_tensor_name = factorization['observed_tensor']
            observed_tensor_xhat_name = 'gtp_hat_'+observed_tensor_name

            update_xhat(gctf_model, update_rules, observed_tensor_xhat_name, factorization)
            
            # generate update rules for this Z_alpha if Z_alpha appears in factorization of X_v
            if ltn in factorization['latent_tensors']:
                other_Z_alpha_tensors = copy.deepcopy(latent_tensor_names)
                other_Z_alpha_tensors.remove(ltn)

                update_d1_Q_v(gctf_model, update_rules, observed_tensor_name, observed_tensor_xhat_name)
                update_d1_delta(gctf_model, update_rules, latent_tensor_names, ltn, observed_tensor_name, other_Z_alpha_tensors)
                update_d1_alpha(gctf_model, update_rules, factorization_index, ltn, factorization)

                update_d2_Q_v(update_rules, observed_tensor_name, observed_tensor_xhat_name, factorization)
                update_d2_delta(gctf_model, update_rules, ltn, observed_tensor_name, other_Z_alpha_tensors)
                update_d2_alpha(update_rules, ltn, factorization_index, factorization)

        # update Z_alpha with d1/d2
        update_Z_alpha(gctf_model, update_rules, ltn)

    return update_rules

def get_beta_divergence(spark, all_tensors_config, x, mu, p):
    assert isinstance(x, basestring) and isinstance(mu, basestring), 'x and mu must be strings containing tensor names in the gctf model'
    # TODO: assert tensors exist,
    # TODOL assert tensors have 'df' element

    assert p==1, 'get_beta_divergence: general beta divergence not implemented yet'

    # faster equation for beta divergence
    if p == 1:
        operation = {
            'operation_type':'hadamard',
            'output':'gtp_beta_divergence',
            'input':{
                'combination_operator':operator.add,
                'arguments':[
                    {
                        'data':mu
                    },
                    {
                        'suboperation':{
                            'combination_operator':operator.sub,
                            'arguments':[
                                {
                                    'suboperation':{
                                        'combination_operator':operator.sub,
                                        'arguments':[
                                            {
                                                'suboperation':{
                                                    'combination_operator':operator.mul,
                                                    'arguments':[
                                                        {
                                                            'data':x
                                                        },
                                                        {
                                                            'data':x,
                                                            'pre_processor':{
                                                                'operator':'log'
                                                            }
                                                        }
                                                    ]
                                                }
                                            },
                                            {
                                                'suboperation':{
                                                    'combination_operator':operator.mul,
                                                    'arguments':[
                                                        {
                                                            'data':x
                                                        },
                                                        {
                                                            'data':mu,
                                                            'pre_processor':{
                                                                'operator':'log'
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                },
                                {
                                    'data':x
                                }
                            ]
                        }
                    }
                ]
            }
        }

    else:
        raise Exception('other p not implemented')

        # # TODO: must take limit of this expression to find the general beta divergence result
    # operation = {
    #     'operation_type':'hadamard',
    #     'output':'gtp_beta_divergence',
    #     'input':{
    #         'combination_operator':operator.sub,
    #         'arguments':[
    #             {
    #                 'suboperation':{
    #                     'combination_operator':operator.truediv,
    #                     'arguments':[
    #                         {
    #                             'data': x,
    #                             'pre_processor': {
    #                                 'operator':'pow',
    #                                 'argument':2-p
    #                             }
    #                         },
    #                         {
    #                             'data':lit((1-p)*(2-p))
    #                         }
    #                     ]
    #                 },
    #             },
    #             {
    #                 'suboperation': {
    #                     'combination_operator':operator.add,
    #                     'arguments':[
    #                         {
    #                             'suboperation':{
    #                                 'combination_operator':operator.truediv,
    #                                 'arguments':[
    #                                     {
    #                                         'suboperation':{
    #                                             'combination_operator':operator.mul,
    #                                             'arguments':[
    #                                                 {
    #                                                     'data':x
    #                                                 },
    #                                                 {
    #                                                     'data':mu,
    #                                                     'pre_processor':{
    #                                                         'operator':'pow',
    #                                                         'argument':1-p
    #                                                     }
    #                                                 }
    #                                             ]
    #                                         }
    #                                     },    
    #                                     {
    #                                         'data':lit(1-p)
    #                                     }
    #                                 ]
    #                             }
    #                         },
    #                         {
    #                             'suboperation':{
    #                                 'combination_operator':operator.truediv,
    #                                 'arguments':[
    #                                     {
    #                                         'data':mu,
    #                                         'pre_processor':{
    #                                             'operator':'pow',
    #                                             'argument':2-p
    #                                         }
    #                                     },
    #                                     {
    #                                         'data':lit(2-p)
    #                                     }
    #                                 ]
    #                             }
    #                         }
    #                     ]
    #                 }
    #             }
    #         ]
    #     }
    # }


    hadamard_df = hadamard(spark, all_tensors_config, operation, output_type='value') #, debug=True)
    #print('was1 %s' %(hadamard_df.collect()) )
    #print('was2 %s' %hadamard_df.groupBy().sum('value'))
    #print('was3 %s' %hadamard_df.groupBy().sum('value').collect())
    #print('was4 %s' %hadamard_df.groupBy().sum('value').collect()[0])
    #print('was5 %s' %hadamard_df.groupBy().sum('value').collect()[0]['sum(value)'])
    return float(hadamard_df.groupBy().sum('value').collect()[0]['sum(value)'])


def calculate_divergence(spark, gctf_model):
    for factorization_index, factorization in enumerate(gctf_model['config']['factorizations']):
        dv = get_beta_divergence(spark, gctf_model['tensors'], factorization['observed_tensor'], 'gtp_hat_'+factorization['observed_tensor'], factorization['p'])
        factorization['divergence_values'].append( dv )
        print('calculate_divergence: factorization_index %s divergence_values %s' %(factorization_index, factorization['divergence_values']))
    # TODO: add assertion for divergence value reduction


def gctf(spark, gctf_model, iteration_num):
    update_rules = gen_update_rules(spark, gctf_model)
    fp = open('/tmp/rules', 'w')
    json.dump( update_rules, fp, indent=4, sort_keys=True, cls=ComplexEncoder )
    fp.close()

    for factorization in gctf_model['config']['factorizations']:
        factorization['divergence_values'] = []

    for epoch_index in range(iteration_num):
        for update_rule in update_rules:
            if update_rule['operation_type'] == 'gtp':
                gtp(spark, update_rule['gtp_spec'], gctf_model, debug=False)
            elif update_rule['operation_type'] == 'hadamard':
                hadamard(spark, gctf_model['tensors'], update_rule, debug=False)
            else:
                raise Exception('unknown opreation_type %s' %update_rule)

        calculate_divergence(spark, gctf_model)

if __name__ == '__main__':
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
        'tensors' : {}
    }

    test_tensors = {
        'gctf_test_X1' : {
            'indices' : [ 'i', 'j' ],
        },
        'gctf_test_X2' : {
            'indices' : [ 'i', 'r' ],
        },
        'gctf_test_Z1' : {
            'indices' : [ 'i', 'k' ],
        },
        'gctf_test_Z2' : {
            'indices' : [ 'k', 'j' ],
        },
        'gctf_test_Z3' : {
            'indices' : [ 'k', 'r' ],
        }
    }
    for tensor_name in test_tensors:
        generate_hdfs_tensor_data( gctf_model['tensors'], gctf_model['config']['cardinalities'], tensor_name, test_tensors[tensor_name]['indices'])

    spark = SparkSession.builder.appName("gtp").getOrCreate()
    # load hdfs data into spark memory
    for tensor_name in gctf_model['tensors']:
        gctf_model['tensors'][tensor_name]['df'] = read_tensor_data_from_hdfs(spark, gctf_model['tensors'], tensor_name, gctf_data_path)
    gctf(spark, gctf_model, 10)

    print( json.dumps( gctf_model, indent=4, sort_keys=True, cls=ComplexEncoder ) )

    spark.stop()
