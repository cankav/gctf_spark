
    
def apply_pre_processor_helper(value, pre_processor_spec):
    if pre_processor_spec['operator'] == 'pow':
        # TODO: use pyspark.sql.functions.pow(col1, col2)
        return pow(value, pre_processor_spec['argument'])
    elif pre_processor_spec['operator'] == 'log':
        return PysparkSQLFunctions.log(value)
    else:
        raise Exception('unknown pre_processor %s' %pre_processor_spec)


def apply_pre_processor(de_prep):
    data_element = de_prep[0]
    if len(de_prep) == 1:
        # no pre_processor
        if isinstance(data_element, DataFrame):
            output_element = data_element.withColumnRenamed('value', 'output')
        elif isinstance(data_element, Column): #is_number(data_element):
            output_element = data_element
        else:
            raise Exception('unknown data element (2)')

    elif len(de_prep) == 2:
        # yes pre_processor
        pre_processor_spec = de_prep[1]
        if isinstance(data_element, DataFrame):
            output_element = data_element.withColumn('output', apply_pre_processor_helper(data_element['value'], pre_processor_spec))
        elif isinstance(data_element, Column): #is_number(data_element):
            output_element = apply_pre_processor_helper(data_element, pre_processor_spec)
        else:
            raise Exception('unknown data element (3)')

    else:
        raise Exception('hadamard: de_prep must have length 1 or 2')

    return output_element


# TODO: remove suboperation - redundant level
#def process_operation(spark, all_tensors_config, input_spec, level=0, debug=False):
def get_hadamard_operation_as_string(rule, level=0):
    # # prepare output element (DataFrame or numeric) for all operands
    # global process_operation_index_set # used for sanity checking on master node

    if level == 0:
        rule_str = ''

    for argument in rule['input']['arguments']:
        if 'suboperation' in argument:
            rule_str.append(process_operation(argument['suboperation'], level=level+1))
        else:
            # fetch data element
            assert 'data' in argument, 'process_operation: if argument is not a suboperation, it must have a data element, this argument does not have it, bad argument bad! %s' % json.dumps(argument, indent=4, sort_keys=True, cls=ComplexEncoder )
            data_element = argument['data'] # can be a string representing a dataframe, can be a scalar numeric value
            if isinstance(data_element, basestring):
                tensor_name = data_element
                assert tensor_name in all_tensors_config, 'tensor_name %s must be located in all_tensors_config %s to treat it as a tensor' %(tensor_name, json.dumps( all_tensors_config, indent=4, sort_keys=True, cls=ComplexEncoder ))
                data_element = all_tensors_config[tensor_name]['df']

                # sanity check
                if process_operation_index_set:
                    assert set(all_tensors_config[tensor_name]['indices']) == process_operation_index_set, 'hadamard: tensor operands must have same set of indices'
                process_operation_index_set = set(all_tensors_config[tensor_name]['indices'])

            elif isinstance(data_element, Column): # or is_number(data_element):
                data_element = data_element

            else:
                raise Exception('unknown data element (1)')

            # add data_element to all_arguments
            if 'pre_processor' in argument:
                all_arguments.append( (data_element, argument['pre_processor'] ) )

            elif 'data' in argument:
                all_arguments.append( (data_element, ) )

            else:
                raise Exception('unknown argument')


    # apply arithmetic operation with pre-processor and return (data_element, pre_processor) pair
    for de_prep_index, de_prep in enumerate(all_arguments):
        pre_processed_de = apply_pre_processor(de_prep) # output_element may be a DataFrame or scalar numeric

        if de_prep_index == 0:
            output_de = pre_processed_de

        else:
            # got 2 elements to merge with input_spec['combination_operator']: output_de and pre_processed_de, each one may be a DataFrame or scalar numeric
            assert input_spec['combination_operator'], 'combination operator can not be None but found %s' %input_spec['combination_operator']
            if isinstance(output_de, DataFrame) and isinstance(pre_processed_de, DataFrame):
                output_de = output_de.join(
                    pre_processed_de,
                    list(process_operation_index_set)
                ).withColumn( 'final_output',
                              input_spec['combination_operator'](output_de.output, pre_processed_de.output) )

            elif not isinstance(output_de, DataFrame) and isinstance(pre_processed_de, DataFrame):
                output_de = pre_processed_de.withColumn('final_output', input_spec['combination_operator'](output_de, pre_processed_de.output))

            elif isinstance(output_de, DataFrame) and not isinstance(pre_processed_de, DataFrame):
                output_de = output_de.withColumn('final_output', input_spec['combination_operator'](output_de.output, pre_processed_de))

            else: # both numeric
                output_de = input_spec['combination_operator'](output_de, pre_processed_de)

    # if there is only 1 argument this is only a pre_processor operation, do not touch value column
    if len(all_arguments) > 1 and isinstance(output_de, DataFrame): # TODO: check isinstance check required?
        output_de = output_de.drop('output', 'value').withColumnRenamed('final_output', 'value')

    return rule_str
