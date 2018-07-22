#from gtp import gtp

cardinalities = {
    'i': 2,
    'j': 3,
    'k': 4
}

gtp_spec = {
    'config': {
        'output' : 'gtp_test_output',
        'inputs' : [ 'gtp_input1', 'gtp_input2' ]
    },
    'tensors' : [
        {
            'name' : 'gtp_test_output',
            'indices' : [ 'i', 'j' ],
        },
        {
            'name' : 'gtp_input1',
            'indices' : [ 'i', 'k' ]
        },
        {
            'name' : 'gtp_input2',
            'indices' :  [ 'j', 'k' ]
        }
    ]
}

# gtp(cardinalities, tensors)
