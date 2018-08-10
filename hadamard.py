from utils import read_tensor_data

def hadamard(spark, gctf_model, update_rule, gctf_data_path='/home/sprk/shared/gctf_data'):

    sc = spark.sparkContext
    rdd = sc.parallelize(xrange(gctf_model['tensors'][update_rule['output']]['numel']))
    for input_spec in gtp_spec['config']['input']['arguments']:
        get_input_value()


        if 'dataframe' not in gctf_model['tensors'][input_tensor_name]:
            gctf_model['tensors'][input_tensor_name]['local_data'] = read_tensor_data(spark, input_tensor_name, gctf_data_path)
            #print ('\n\n\n\n')
            #print(gtp_spec['tensors'][input_tensor_name]['local_data'])
            #print ('\n\n\n\n')
        else:
            print('info: Not re-initializing %s' %input_tensor_name)

