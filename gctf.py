#from gtp import gtp
import operator
import copy
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from gtp import gtp

def get_observed_tensor_names_of_latent_tensor(gctf_model, ltn):
    otn_with_ltn=[]
    for factorization in gctf_model['config']['factorizations']:
        if ltn in factorization['latent_tensors']:
            otn_with_ltn.append(factorization['observed_tensor'])

    assert len(otn_with_ltn), ('ltn %s was not found in any of the factorizations' %ltn)
    return otn_with_ltn

def update_d1_Q_v(gctf_model, update_rules, observed_tensor_name, observed_tensor_xhat_name):
    update_rules.append( {
        'operation_type':'hadamard',
        'output':'_gtp_d1_Q_v_'+observed_tensor_name,
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
        'gtp_spec':gen_gtp(gctf_model, '_gtp_d1_delta_'+ltn, ['_gtp_d1_Q_v_'+observed_tensor_name] + other_Z_alpha_tensors)
    } )

def update_d2_Q_v(update_rules, observed_tensor_name, observed_tensor_xhat_name, factorization):
    update_rules.append( {
        'operation_type':'hadamard',
        'output':'_gtp_d2_Q_v_'+observed_tensor_name,
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
        'gtp_spec':gen_gtp(gctf_model, '_gtp_d2_delta_'+ltn, ['_gtp_d2_Q_v_'+observed_tensor_name] + other_Z_alpha_tensors)
    } )

def update_d2_alpha(update_rules, ltn, factorization_index, factorization):
    if factorization_index == 0:
        update_rules.append( {
            #{end+1} = { '=', obj.d2_alpha(alpha), ['obj.config.tfmodel.phi_vector(' num2str(v_index) ')^-1 .* obj.config.tfmodel.d2_delta(' num2str(alpha) ').data'] };
            'operation_type':'hadamard',
            'output':'_gtp_d2_alpha_'+ltn+'_v_'+factorization['observed_tensor'],
            'input':{
                'combination_operator':operator.mul, #input must be scalar or same size as output
                'arguments':[
                    {
                        'data':factorization['phi'],
                        'pre_processor':{
                            'operator':'pow',
                            'argument':-1
                        }
                    },
                    {
                        'data':'_gtp_d2_delta_'+ltn
                    }
                ]
            }
        } )

    else:
        update_rules.append( {
            #{end+1} = { '=', obj.d2_alpha(alpha), ['obj.config.tfmodel.d2_alpha(' num2str(alpha) ').data + obj.config.tfmodel.phi_vector(' num2str(v_index) ')^-1 .* obj.config.tfmodel.d2_delta(' num2str(alpha) ').data'] };
            'operation_type':'hadamard',
            'output':'_gtp_d2_alpha_'+ltn+'_v_'+factorization['observed_tensor'],
            'input':{
                'combination_operator':operator.add,
                'arguments':[
                    {
                        'data':'_gtp_d2_alpha_'+ltn
                    },
                    {
                        'suboperation':{ # this input element is calculated using an expression
                            'combination_operator':operator.mul,
                            'arguments':[
                                {
                                    'data':factorization['phi'],
                                    'pre_processor':{
                                        'operator':'pow',
                                        'argument':-1
                                    }
                                },
                                {
                                    'data':'_gtp_d2_delta_'+ltn
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
            'output':'_gtp_d1_alpha_'+ltn+'_v_'+factorization['observed_tensor'],
            'input':{
                'combination_operator':operator.mul, #input must be scalar or same size as output
                'arguments':[
                    {
                        'data':factorization['phi'],
                        'pre_processor':{
                            'operator':'pow',
                            'argument':-1
                        }
                    },
                    {
                        'data':'_gtp_d1_delta_'+ltn
                    }
                ]
            }
        } )

    else:
        update_rules.append( {
            'operation_type':'hadamard',
            'output':'_gtp_d1_alpha_'+ltn+'_v_'+factorization['observed_tensor'],
            'input':{
                'combination_operator':operator.add,
                'arguments':[
                    {
                        'data':'_gtp_d1_alpha_'+ltn
                    },
                    {
                        'suboperation':{ # this input element is calculated using an expression
                            #obj.config.tfmodel.phi_vector(' num2str(v_index) ')^-1 .* obj.config.tfmodel.d1_delta(' num2str(alpha) ').data'
                            'combination_operator':operator.mul,
                            'arguments':[
                                {
                                    'data':factorization['phi'],
                                    'pre_processor':{
                                        'operator':'pow',
                                        'argument':-1
                                    }
                                },
                                {
                                    'data':'_gtp_d1_delta_'+ltn
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
        'gtp_spec' : gen_gtp(gctf_model, observed_tensor_xhat_name, factorization['latent_tensors'])
    } )


def update_Z_alpha(gctf_model, update_rules, ltn):
    # { '=', obj.Z_alpha(alpha), ['obj.config.tfmodel.Z_alpha(' num2str(alpha) ').data .* obj.config.tfmodel.d1_alpha(' num2str(alpha) ').data ./ obj.config.tfmodel.d2_alpha('  num2str(alpha) ').data'] };
    rule = {
        'operation_type':'hadamard',
        'output':ltn,
        'input':[
            {
                'data':ltn
            },
            {
                'suboperation':{
                    'combination_operator':operator.div,
                    'arguments':None
                }
            }
        ]
    }

    otn_with_ltn=get_observed_tensor_names_of_latent_tensor(gctf_model, ltn)
    if len(otn_with_ltn) == 1:
        rule['input'][1]['suboperation']['arguments'] = [
            {'data':'_gtp_d1_alpha_'+ltn+'_v_'+otn_with_ltn[0]},
            {'data':'_gtp_d2_alpha_'+ltn+'_v_'+otn_with_ltn[0]}
        ]
    else:
        rule['input'][1]['suboperation']['arguments'] = [
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
            rule['input'][1]['suboperation']['arguments'][0]['suboperation']['arguments'].append(
                {'data':'_gtp_d1_alpha_'+ltn+'_v_'+otn},
            )

            rule['input'][1]['suboperation']['arguments'][1]['suboperation']['arguments'].append(
                {'data':'_gtp_d2_alpha_'+ltn+'_v_'+otn},
            )

        update_rules.append(rule)


def generate_tensor(gctf_model, tensor_name, indices):
    assert tensor_name not in gctf_model['tensors'], 'tensor_name %s already exists in gctf_model %s' %(tensor_name, gctf_model)

    gctf_model['tensors'][tensor_name] = {
        'indices' : indices
    }


def gen_gtp(gctf_model, output_tensor_name, input_tensor_names):
    gtp_spec = {
        'config' : {
            'cardinalities' : gctf_model['config']['cardinalities'],
            'output' : output_tensor_name,
            'input' : input_tensor_names
        },
        'tensors' : {
            output_tensor_name : gctf_model['tensors'][output_tensor_name]
        }
    }

    for itn in input_tensor_names:
        gtp_spec['tensors'][itn] = gctf_model['tensors'][itn]

    return gtp_spec

def gen_update_rules(gctf_model):
    # create intermediate tensors
    latent_tensor_names = set()
    for factorization in gctf_model['config']['factorizations']:
        observed_tensor_name = factorization['observed_tensor']
        generate_tensor(gctf_model, '_gtp_hat_'+observed_tensor_name, gctf_model['tensors'][observed_tensor_name]['indices'])

        generate_tensor(gctf_model, '_gtp_d1_Q_v_'+observed_tensor_name, gctf_model['tensors'][observed_tensor_name]['indices'])
        generate_tensor(gctf_model, '_gtp_d2_Q_v_'+observed_tensor_name, gctf_model['tensors'][observed_tensor_name]['indices'])

        for ltn in factorization['latent_tensors']:
            latent_tensor_names.add( ltn )

            generate_tensor(gctf_model, '_gtp_d1_alpha_'+ltn+'_v_'+observed_tensor_name, gctf_model['tensors'][ltn]['indices'])
            generate_tensor(gctf_model, '_gtp_d2_alpha_'+ltn+'_v_'+observed_tensor_name, gctf_model['tensors'][ltn]['indices'])


    latent_tensor_names = list(latent_tensor_names)
    for ltn in latent_tensor_names:
        generate_tensor(gctf_model, '_gtp_d1_delta_'+ltn, gctf_model['tensors'][ltn]['indices'])
        generate_tensor(gctf_model, '_gtp_d2_delta_'+ltn, gctf_model['tensors'][ltn]['indices'])

    # get all indices
    all_indices = set()
    for tensor_name, tensor in gctf_model['tensors'].iteritems():
        all_indices.update( tensor['indices'] )
    # all_indices must have fixed ordering
    all_indices = list(all_indices)

    # add full tensor to the tensor list
    gctf_model['tensors']['_gtp_full_tensor'] = {'indices':all_indices}

    # indices are assumed to be serialized left to right
    for tensor_name, tensor in gctf_model['tensors'].iteritems():
        tensor['strides'] = []
        current_stride = 1
        tensor['numel'] = 1
        for tensor_index_name in tensor['indices']:
            tensor['strides'].append(current_stride)
            current_stride *= gctf_model['config']['cardinalities'][tensor_index_name]
            tensor['numel'] *= gctf_model['config']['cardinalities'][tensor_index_name]


    update_rules = []
    # update each Z_alpha
    for ltn in latent_tensor_names:
        # update each X_hat
        for factorization_index, factorization in enumerate(gctf_model['config']['factorizations']):
            observed_tensor_name = factorization['observed_tensor']
            observed_tensor_xhat_name = '_gtp_hat_'+observed_tensor_name

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

    print(update_rules)
    return update_rules

def gctf_epoch(spark, gctf_model, iteration_num):
    for update_rule in gen_update_rules(gctf_model):
        if update_rule['operation_type'] == 'gtp':
            gtp(spark, update_rule['gtp_spec'])
        elif update_rule['operation_type'] == 'hadamard':
            hadamard(spark, gctf_model, update_rule)
        else:
            raise Exception('unknown opreation_type %s' %update_rule)

    print update_rules

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
                    'observed_tensor' : 'X1',
                    'latent_tensors' : [ 'Z1', 'Z2' ],
                    'p' : 1,
                    'phi' : 1
                },
                {
                    'observed_tensor' : 'X2',
                    'latent_tensors' : [ 'Z1', 'Z3' ],
                    'p' : 1,
                    'phi' : 1
                }
            ]
        },
        'tensors' : {
            'X1' : {
                'indices' : [ 'i', 'j' ]
            },
            'X2' : {
                'indices' : [ 'i', 'r' ]
            },
            'Z1' : {
                'indices' : [ 'i', 'k' ]
            },
            'Z2' : {
                'indices' : [ 'k', 'j' ]
            },
            'Z3' : {
                'indices' : [ 'k', 'r' ]
            }
        }
    }

    spark = SparkSession.builder.appName("gtp").getOrCreate()
    gctf_epoch(spark, gctf_model, 10)
    spark.stop()
