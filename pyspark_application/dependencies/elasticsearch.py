import json


def save_dataframes_to_elasticsearch(dataframe, index, es_write_config):
    """
       Helper function to save PySpark DataFrames to elasticsearch cluster

       Parameters
       ----------

       dataframes: list of all PySpark DataFrames
       indices: list of elasticsearch indices
       es_write_config: dict of elasticsearch write config
    """

    print("Processing index:", index)

    es_write_config['es.resource'] = index

    rdd_ = dataframe.rdd
    rdd_.map(lambda row: (None,
                          json.dumps(row.asDict()))) \
        .saveAsNewAPIHadoopFile(path='-',
                                outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                                keyClass="org.apache.hadoop.io.NullWritable",
                                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                                conf=es_write_config)
