from utils import read_tensor_data_from_hdfs
from utils import gctf_data_path
from utils import ComplexEncoder
import json
from utils import generate_hdfs_tensor_data
import operator
from utils import create_full_tensor
from utils import full_tensor_name

def gtp(spark, gtp_spec, gctf_model=None, debug=False):
    print('EXECUTING RULE: starting GTP operation gtp_spec %s' %json.dumps( gtp_spec, indent=4, sort_keys=True, cls=ComplexEncoder ))

    if full_tensor_name not in gtp_spec['tensors']:
        create_full_tensor(spark, gtp_spec['tensors'], gtp_spec['config']['cardinalities'])
        gtp_spec['tensors'][full_tensor_name]['df'] = read_tensor_data_from_hdfs(spark, gtp_spec['tensors'], full_tensor_name, gctf_data_path)
    else:
        # clear full tensor elements not necessary because all entries will be re-calculated
        print('gtp: full tensor for gtp already created')
        pass

    # Calculate F tensor
    assert 'df' in gtp_spec['tensors'][full_tensor_name], 'can not work with an uninitialized tensor. tensor spec: %s' %json.dumps( gtp_spec['tensors'][full_tensor_name], indent=4, sort_keys=True, cls=ComplexEncoder )
    F_df = gtp_spec['tensors'][full_tensor_name]['df']
    multiply_column_list = []
    for input_tensor_name in gtp_spec['config']['input']:
        F_df = F_df.join( gtp_spec['tensors'][input_tensor_name]['df'], gtp_spec['tensors'][input_tensor_name]['indices'], 'inner' )
        multiply_column_list.append(gtp_spec['tensors'][input_tensor_name]['df'].value)
    F_df = F_df.withColumn('f_tensor_value', reduce( operator.mul, multiply_column_list ))
    F_df.localCheckpoint()

    # Calculate output tensor
    output_tensor_name = gtp_spec['config']['output']
    output_df = F_df.groupBy(gtp_spec['tensors'][output_tensor_name]['indices']).sum('f_tensor_value').withColumnRenamed('sum(f_tensor_value)', 'value')

    gtp_spec['tensors'][output_tensor_name]['df'] = output_df

    if debug:
        output_values = output_df.collect()
        print('gtp: gtp_spec %s output_values %s' %(json.dumps( gtp_spec, indent=4, sort_keys=True, cls=ComplexEncoder ), output_values))

        for row in output_values:
            if row['value'] is None:
                print( 'found None value in output in row %s printing input tensor values' %str(row))
                print(gtp_spec['tensors'])
                for tensor_name in gtp_spec['tensors']:
                    print('tensor %s values %s' %(tensor_name, gtp_spec['tensors'][tensor_name]['df'].collect()))
                raise Exception('found None in hadamard')


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    import os
    gtp_spec = {
        'config': {
            'cardinalities' : {
                'i': 2,
                'j': 3,
                'k': 4
            },
            'output' : 'gtp_test_output',
            'input' : [ 'gtp_test_input1', 'gtp_test_input2' ]
        },
        'tensors' : {
            'gtp_test_output' : {
                'indices' : [ 'i', 'j' ]
            },
            'gtp_test_input1' : {
                'indices' : [ 'i', 'k' ],
                'local_filename' : '/home/sprk/shared/gctf_data/gtp_test_input1.csv'
            },
            'gtp_test_input2' : {
                'indices' :  [ 'j', 'k' ],
                'local_filename' : '/home/sprk/shared/gctf_data/gtp_test_input2.csv',
            }
        }
    }

    spark = SparkSession.builder.appName("gtp").getOrCreate()

    # put input local data onto hdfs
    local_files_str=''
    for tensor_name in gtp_spec['tensors']:
        if 'local_filename' in gtp_spec['tensors'][tensor_name]:
            local_files_str+=' ' + gtp_spec['tensors'][tensor_name]['local_filename']
    cmd = "$HADOOP_HOME/bin/hadoop fs -put %s %s" %(local_files_str, gctf_data_path)
    print( cmd )
    os.system( cmd )

    # load hdfs data into spark memory
    for tensor_name in gtp_spec['config']['input']:
        if 'local_filename' in gtp_spec['tensors'][tensor_name]:
            gtp_spec['tensors'][tensor_name]['hdfs_filename'] = '/'.join([gctf_data_path, gtp_spec['tensors'][tensor_name]['local_filename'].split('/')[-1]])
            gtp_spec['tensors'][tensor_name]['df'] =  read_tensor_data_from_hdfs(spark, gtp_spec['tensors'], tensor_name, gctf_data_path)

    gtp(spark, gtp_spec)

    output_tensor_name = gtp_spec['config']['output']
    print('gtp: computed output')
    gtp_spec['tensors'][output_tensor_name]['df'].show(n=1000)

    for row in gtp_spec['tensors'][output_tensor_name]['df'].collect():
        if row.i == 1 and row.j == 1:
            assert row['value'] == 11800, 'wrong output %s' %str(row)
        elif row.i == 1 and row.j == 2:
            assert row['value'] == 13400, 'wrong output %s' %str(row)
        elif row.i == 1 and row.j == 3:
            assert row['value'] == 15000, 'wrong output %s' %str(row)
        elif row.i == 2 and row.j == 1:
            assert row['value'] == 14000, 'wrong output %s' %str(row)
        elif row.i == 2 and row.j == 2:
            assert row['value'] == 16000, 'wrong output %s' %str(row)
        elif row.i == 2 and row.j == 3:
            assert row['value'] == 18000, 'wrong output %s' %str(row)
        else:
            raise Exception('unexpected index values %s' %str(row))

    print('gtp: computed correct output')

    spark.stop()

# >> one*two
#ans =        11800       13400       15000
#             14000       16000       18000
# i, j, value
# 1, 1, 118000
# 2, 1, 140000

# +---+---+-------------------+
# |  i|  j|value|
# +---+---+-------------------+
# |  1|  1|            11800.0|
# |  1|  2|            13400.0|
# |  1|  3|            15000.0|
# |  2|  1|            14000.0|
# |  2|  2|            16000.0|
# |  2|  3|            18000.0|
# +---+---+-------------------+
