from utils import read_tensor_data
from utils import linear_index_to_tensor_index

def hadamard(spark, gctf_model, update_rule, gctf_data_path='/home/sprk/shared/gctf_data'):
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
        
    def calculate_output_value(input_config):
      #-> dataframe.filter
      
      

    def compute_output_element(output_tensor_linear_index):
        output_tensor_indices = linear_index_to_tensor_index(gctf_model['tensors'], gctf_model['config']['cardinalities'], output_tensor_linear_index, update_rule['output'])

        return calculate_output_value(output_tensor_indices, update_rule['input'])

    sc = spark.sparkContext
    rdd = sc.parallelize(xrange(gctf_model['tensors'][update_rule['output']]['numel']))
    rdd1 = rdd.map(compute_output_element)
