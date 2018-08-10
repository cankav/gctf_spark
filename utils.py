import os.path

def read_tensor_data(spark_session, tensor_name, root_path, cache=True):
    filename='/'.join([root_path,tensor_name])+'.csv'
    assert os.path.isfile(filename), 'read_tensor_data: File %s does not exist, aborting'%filename
    tensor_data = spark_session.read.load(filename, format="csv", sep=",", inferSchema="true", header="true")
    if cache:
        tensor_data.cache()
    return tensor_data
