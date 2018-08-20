from utils import linear_index_to_tensor_index
import operator
import functools
from utils import gctf_data_path

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


  def get_input_data_values(input_config, output_element_DOK):
    resolved_arguments = []
    for argument in input_config['arguments']:
      if 'data' in argument:
        input_data_value = gctf_model['tensors'][argument['data']]['local_data']
        for index_name in output_element_DOK:
          input_data_value = input_data_value.filter(str(index_name) + '=' + str(output_element_DOK[index_name]))

        if 'pre_processor' in argument:
          if argument['pre_processor'] == 'pow':
            resolved_arguments.append( pow(input_data_value, argument['pre_processor']['argument']) )
          else:
            raise Exception('unknown pre_processor %s' %input_config)
        else:
          resolved_arguments.append( input_data_value )

      elif 'suboperation' in argument:
        resolved_arguments.extend( compute_output_element_helper(argument['suboperation'], output_element_DOK) )

    return resolved_arguments
  
  def compute_output_element_helper(input_config, output_element_DOK):
    if input_config['combination_operator'] in [operator.truediv]:
      assert len(input_config['arguments']) == 2, 'only can divide 2 input elements'
    if input_config['combination_operator'] is None:
      assert len(input_config['arguments']) == 1, 'only can output the input if combination_operator is None'

    get_input_data_values()

    if input_config['combination_operator'] == operator.truediv:
      return operator.truediv(resolved_arguments[0], resolved_arguments[1])
    elif input_config['combination_operator'] is None:
      assert len(resolved_arguments) == 1, 'resolved_arguments must have 1 argument when combination_operator is None, programming error'
      return resolved_arguments[0]
    elif input_config['combination_operator'] == [operator.mul, operator.add]:
      return functools.reduce( input_config['combination_operator'], resolved_arguments )
    else:
      raise Exception('unknown combination_operator %s' %input_config)

  def compute_output_element(full_tensor_linear_index):
    output_element_DOK = linear_index_to_tensor_index(gctf_model['tensors'], gctf_model['cardinalities'], full_tensor_linear_index, update_rule['output'])
    return compute_output_element_helper(update_rule['input'], output_element_DOK)

  sc = spark.sparkContext
  rdd = sc.parallelize(xrange(gctf_model['tensors'][update_rule['output']]['numel']))
  rdd1 = rdd.map(compute_output_element)
