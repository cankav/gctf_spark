from utils import read_tensor_data

def compute_output_element(output_tensor_linear_index):
    for argument in gtp_spec['config']['input']['arguments']:
        get_input_value(argument)
    

def hadamard(spark, gctf_model, update_rule, gctf_data_path='/home/sprk/shared/gctf_data'):
    sc = spark.sparkContext
    rdd = sc.parallelize(xrange(gctf_model['tensors'][update_rule['output']]['numel']))
    rdd1 = rdd.map(compute_output_element)
