from gtp import gtp

gtp_spec = {
    'config': {
        'cardinalities' : {
            'i': 2,
            'j': 3,
            'k': 4
        },
        'output' : 'gtp_test_output',
        'inputs' : [ 'gtp_input1', 'gtp_input2' ]
    },
    'tensors' : [
        {
            'name' : 'gtp_test_output',
            'indices' : [ 'i', 'j' ],
        },
        {
            'name' : 'gtp_test_input1',
            'indices' : [ 'i', 'k' ]
        },
        {
            'name' : 'gtp_test_input2',
            'indices' :  [ 'j', 'k' ]
        }
    ]
}

gtp(gtp_spec)
