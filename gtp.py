from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession

from utils import *

def gtp(gtp_spec):
    
    # get all indices
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
        tensor['max_numel'] = 1
        for tensor_index_name in tensor['indices']:
            tensor['strides'].append(current_stride)
            current_stride *= gtp_spec['config']['cardinalities'][tensor_index_name]
            tensor['max_numel'] *= gtp_spec['config']['cardinalities'][tensor_index_name]

    print ('gtp_spec %s' % gtp_spec)

    def mapfunc(full_tensor_index):
        result = None
        for input_tensor_name in gtp_spec['config']['inputs']:
            tensor_data = spark.read.load(gctf_data_path+input_tensor_name+'.csv', format="csv", sep=",", inferSchema="true", header="true")
            tensor_data.cache()
            # calculate address for each input tensor and return product of all as result
            if result is None:
                result = input tensor element
            else:
                result *= input tensor element
        return (full_tensor_index, result)

        tensor_strides = strides[ row.__fields__['tensor_name'] ] field in row.__fields__:
        if field != 'tensor_name' and field != 'value':
            full_index += tensor_strides[field] * row[field]

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
    spark = SparkSession.builder.appName("gtp").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize(xrange(gtp_spec['tensors'][get_tensor_index(gtp_spec, '_gtp_full_tensor')]['max_numel']))
    print rdd.map(mapfunc).collect()

    spark.stop()
