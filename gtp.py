from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession

def gtp(gtp_spec):
    #logFile = "/home/sprk/shared/file.txt"  # Should be some file on your system

    #x_data_path = '/home/sprk/shared/coupled_dataset/AAPL/observation_data-AAPL-30-days-1000-words-268-documents-RS.csv'

    spark = SparkSession.builder.appName("gtp").getOrCreate()

    #df = spark.read.load(x_data_path, format="csv", sep=",", inferSchema="true", header="true").cache()

    #df.summary().show()

    #df.printSchema()
    # get all indices
    all_indices = set()
    for tensor in [ tensors['output'] ] + tensors['input']:
        all_indices.update( tensor['indices'] )
    # all_indices must have fixed ordering
    all_indices = list(all_indices)

    # add full tensor to the tensor list
    tensors['_gtp_full_tensor'] = { 'indices' : all_indices }

    # indices are assumed to be serialized left to right
    for tensor_name in index_orderings:
        tensor_strides[tensor_name] = {}
        current_stride = 1
        tensor_max_numels[tensor_name] = 1
        for tensor_index_name in index_orderings[tensor_name]:
            tensor_strides[tensor_name][tensor_index_name] = current_stride
            current_stride *= cardinalities[tensor_index_name]
            tensor_max_numels[tensor_name] *= cardinalities[tensor_index_name]

    print ('tensor_strides %s' % tensor_strides)
    print ('tensor_max_numels %s' % tensor_max_numels)

    def mapfunc(row):
        full_index = 0
        tensor_strides = strides[ row.__fields__['tensor_name'] ]
        for field in row.__fields__:
            if field != 'tensor_name' and field != 'value':
                full_index += tensor_strides[field] * row[field]

    #print ( df.rdd.map(mapfunc).collect() )

    spark.stop()



    #conf = SparkConf().setAppName('gtp')
    #sc = SparkContext(conf=conf)
    #distFile = sc.textFile(logFile)
    #distFile.persist()
    #lineLengths = distFile.map(lambda s: len(s))
    #totalLength = lineLengths.reduce(lambda a, b: a + b)
    #sc.stop()


