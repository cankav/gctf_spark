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
}
