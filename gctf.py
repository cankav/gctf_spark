from gtp import gtp
import operator

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

def gen_gctf_rules(gctf_model):
    # create intermediate tensors
    latent_tensor_names = set()
    for factorization in gctf_model['config']['factorizations']:
        observed_tensor_name = factorization['observed_tensor']
        generate_tensor(gctf_model, '_gtp_hat_'+observed_tensor_name, gctf_model['tensors'][observed_tensor_name]['indices'])

        generate_tensor(gctf_model, '_gtp_d1_Q_v_'+observed_tensor_name, gctf_model['tensors'][observed_tensor_name]['indices'])

        for ltn in factorization['latent_tensors']:
            latent_tensor_names.add( ltn )

    latent_tensor_names = list(latent_tensor_names)
    for ltn in latent_tensor_names:
        generate_tensor(gctf_model, '_gtp_d1_alpha_'+ltn, gctf_model['tensors'][ltn]['indices'])
        generate_tensor(gctf_model, '_gtp_d2_alpha_'+ltn, gctf_model['tensors'][ltn]['indices'])

    update_rules = []
    # update each Z_alpha
    for ltn in latent_tensor_names:
        # update each X_hat
        for factorization in gctf_model['config']['factorizations']:
            observed_tensor_name = factorization['observed_tensor']
            observed_tensor_xhat_name = '_gtp_hat_'+observed_tensor_name
            update_rules.append(
                {
                    'operation_type':'gtp',
                    'gtp_spec' : gen_gtp(gctf_model, observed_tensor_xhat_name, factorization['latent_tensors'])
                }
            )

            # update d1_Q_v if Z_alpha appears in factorization of X_v
            if ltn in factorization['latent_tensors']:
                update_rules.append(
                    {
                        'operation_type':'hadamard',
                        'output':'_gtp_d1_Q_v_'+observed_tensor_name,
                        'input':{
                            'combination_operator':operator.mul,
                            'arguments':[
                                {'data':observed_tensor_name},
                                {'data':observed_tensor_xhat_name,
                                 'pre_processor': {
                                     'operator':'pow',
                                     'argument':-1
                                 }
                                }
                            ]
                        }
                    }
                )

def gctf_epoch(gctf_model, iteration_num):
    gen_gctf_rules()

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

    gctf_epoch(gctf_model, 10)
