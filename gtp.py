from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from operator import add

def get_tensor_index(gtp_spec, tensor_name):
    for tensor_index, tensor in enumerate(gtp_spec['tensors']):
        if tensor['name'] == tensor_name:
            return tensor_index

    return None

def read_tensor_data(spark_session, tensor_name, cache=True, root_path='/home/sprk/shared/gctf_data/'):
    tensor_data = spark_session.read.load(root_path+tensor_name+'.csv', format="csv", sep=",", inferSchema="true", header="true")
    if cache:
        tensor_data.cache()
    return tensor_data

def find_tensor_value(gtp_spec, input_tensor_name, tensor_index_values):
    tensor_spec = gtp_spec['tensors'][get_tensor_index(gtp_spec, input_tensor_name)]
    tensor_data = tensor_spec['local_data']
    for row in tensor_data:
        matched_index_count = 0
        for tensor_index_name in tensor_spec['indices']:
            if row[tensor_index_name] == tensor_index_values[tensor_index_name]:
                matched_index_count += 1
        if matched_index_count == len(tensor_spec['indices']):
            return row['value']
    return None

def linear_index_to_tensor_index( gtp_spec, linear_index, linear_index_tensor_name, tensor_index_tensor_name=None, as_dict=True ):
    current_stride = 1
    tensor_index_values_dict = {}
    if tensor_index_tensor_name is None:
        tensor_index_tensor_name = linear_index_tensor_name
    tensor_index_tensor_indices = gtp_spec['tensors'][get_tensor_index(gtp_spec, tensor_index_tensor_name)]['indices']
    for index_name in gtp_spec['tensors'][get_tensor_index(gtp_spec, linear_index_tensor_name)]['indices']:
        index_cardinality = gtp_spec['config']['cardinalities'][index_name]
        value = ((linear_index / current_stride) % index_cardinality) + 1
        current_stride *= index_cardinality
        if index_name in tensor_index_tensor_indices:
            tensor_index_values_dict[index_name] = value

    if as_dict:
        return tensor_index_values_dict
    else:
        # make sure ordering is in 'tensor_index_tensor_name' indices order
        tensor_index_values_list = []
        for index_name in tensor_index_tensor_indices:
            tensor_index_values_list.append(tensor_index_values_dict[index_name])
        return tuple(tensor_index_values_list)

def gtp(spark, gtp_spec):
    
    # get all indices
    full_tensor_name = '_gtp_full_tensor'
    if get_tensor_index(gtp_spec, full_tensor_name) is None:
        all_indices = set()
        for tensor in gtp_spec['tensors']:
            all_indices.update( tensor['indices'] )
        # all_indices must have fixed ordering
        all_indices = list(all_indices)

    # add full tensor to the tensor list
    gtp_spec['tensors'].append( {'name':'_gtp_full_tensor', 'indices':all_indices} )

    # indices are assumed to be serialized left to right
    for tensor in gtp_spec['tensors']:
        tensor['strides'] = []
        current_stride = 1
        tensor['numel'] = 1
        for tensor_index_name in tensor['indices']:
            tensor['strides'].append(current_stride)
            current_stride *= gtp_spec['config']['cardinalities'][tensor_index_name]
            tensor['numel'] *= gtp_spec['config']['cardinalities'][tensor_index_name]

    print ('gtp_spec %s' % gtp_spec)

    # map function to calculate each entry of the full tensor and compute one or more results output tensor indices
    def mapfunc(full_tensor_linear_index):
        print( '\n\nf %s' %full_tensor_linear_index)
        # calculate dimension indices from linear index,
        full_tensor_index_values = linear_index_to_tensor_index( gtp_spec, full_tensor_linear_index, full_tensor_name)

        # fetch corresponding input tensor values
        # multiply them and return (linear index, output_value)
        output_value = None
        for input_tensor_name in gtp_spec['config']['inputs']:
            tensor_config_index = get_tensor_index(gtp_spec, input_tensor_name)
            input_tensor_value = find_tensor_value(gtp_spec, input_tensor_name, full_tensor_index_values) #spark.sql(query)
            if input_tensor_value is not None:
                if output_value is None:
                    output_value = input_tensor_value
                else:
                    output_value *= input_tensor_value
        print( 'full_tensor_index_values %s' %full_tensor_index_values )
        print( 'output_value %s' %output_value )
        if output_value is None:
            return None
        else:
            # convert full_tensor_linear_index to output full index
            output_tensor_index = linear_index_to_tensor_index( gtp_spec, full_tensor_linear_index, full_tensor_name, gtp_spec['config']['output'], as_dict=False )
            return (output_tensor_index, output_value)

    #logFile = "/home/sprk/shared/file.txt"  # Should be some file on your system
    #x_data_path = '/home/sprk/shared/coupled_dataset/AAPL/observation_data-AAPL-30-days-1000-words-268-documents-RS.csv'
    #df.printSchema()

    #conf = SparkConf().setAppName('gtp')
    #sc = SparkContext(conf=conf)
    #distFile = sc.textFile(logFile)
    #distFile.persist()
    #lineLengths = distFile.map(lambda s: len(s))
    #totalLength = lineLengths.reduce(lambda a, b: a + b)
    #sc.stop()

    gctf_data_path = '/home/sprk/shared/gctf_data'
    sc = spark.sparkContext
    rdd = sc.parallelize(xrange(gtp_spec['tensors'][get_tensor_index(gtp_spec, '_gtp_full_tensor')]['numel']))
    for input_tensor_name in gtp_spec['config']['inputs']:
        tensor_config_index = get_tensor_index(gtp_spec, input_tensor_name)
        if 'dataframe' not in gtp_spec['tensors'][tensor_config_index]:
            tensor_data = read_tensor_data(spark, input_tensor_name)
            #tensor_data.createOrReplaceTempView(input_tensor_name)
            gtp_spec['tensors'][tensor_config_index]['local_data'] = tensor_data.rdd.collect()
            #print ('\n\n\n\n')
            #print(gtp_spec['tensors'][tensor_config_index]['local_data'])
            #print ('\n\n\n\n')
        else:
            print('info: Not re-initializing %s' %input_tensor_name)

    rdd1 = rdd.map(mapfunc)
    print( rdd1.collect() )
    return rdd1.reduceByKey(add).collect()


if __name__ == '__main__':
    gtp_spec = {
        'config': {
            'cardinalities' : {
                'i': 2,
                'j': 3,
                'k': 4
            },
            'output' : 'gtp_test_output',
            'inputs' : [ 'gtp_test_input1', 'gtp_test_input2' ]
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

    spark = SparkSession.builder.appName("gtp").getOrCreate()
    print( gtp(spark, gtp_spec) )
    spark.stop()
