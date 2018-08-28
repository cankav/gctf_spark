from utils import linear_index_to_tensor_index
import operator
import functools
from utils import gctf_data_path
from utils import ComplexEncoder
import json

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

  def dok_filter(row, output_element_DOK):
    for index in output_element_DOK:
      try:
        if row[index] != output_element_DOK[index]:
          return False
      except ValueError as e:
        print('row %s output_element_DOK %s' %(row, output_element_DOK))
        raise(e)
    return True

  def get_input_data_values(input_config, output_element_DOK):
    resolved_arguments = []
    for argument in input_config['arguments']:
      if 'data' in argument:
        #input_data_value = gctf_model['tensors'][argument['data']]['local_data']
        #for index_name in output_element_DOK:
        #  print ('WAAS')
        #  print(type(input_data_value))
        #  print(input_data_value)
        #input_data_value = input_data_value.filter(str(index_name) + '=' + str(output_element_DOK[index_name]))

        assert 'data_local' in gctf_model['tensors'][argument['data']], '%s does not have data_local field, not initialized?' %argument['data'] 

        # TODO: replace with rdd/df search
        input_data_value = filter(lambda row: dok_filter(row, output_element_DOK), gctf_model['tensors'][argument['data']]['data_local'])
        assert len(input_data_value) == 1, 'input_data_value must have exactly 1 element, found %s elements, output_element_DOK %s searching in argument key %s argument value %s' %(len(input_data_value), output_element_DOK, argument['data'], gctf_model['tensors'][argument['data']])
        input_data_value = input_data_value[0].value

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

    resolved_arguments = get_input_data_values(input_config, output_element_DOK)

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
    output_element_DOK = linear_index_to_tensor_index(gctf_model['tensors'], gctf_model['config']['cardinalities'], full_tensor_linear_index, update_rule['output'])
    return compute_output_element_helper(update_rule['input'], output_element_DOK)

  print('EXECUTING RULE: starting hadamard operation update_rule %s' %json.dumps( update_rule, indent=4, sort_keys=True, cls=ComplexEncoder ))

  sc = spark.sparkContext
  rdd = sc.parallelize(xrange(gctf_model['tensors'][update_rule['output']]['numel'])).map(compute_output_element)

  df = spark.createDataFrame(rdd)
  if 'filename' in gtp_spec['tensors'][output_tensor_name]:
      filename = gtp_spec['tensors'][output_tensor_name]['filename']
  else:
      filename = '/'.join([gctf_data_path,output_tensor_name])+'.csv'
      gtp_spec['tensors'][output_tensor_name]['filename'] = filename
  #print('WAS')
  #print(rdd.collect())
  df.write.csv(filename, mode='overwrite')

  #print( 'DF' )
  #print(df.count(), len(df.columns))
  #rdd1 = rdd.map(mapfunc)
  #rdd2 = rdd1.reduceByKey(add)
  return (df, filename)
