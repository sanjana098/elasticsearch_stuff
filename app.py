import pandas as pd
from time import time
from elasticsearch import Elasticsearch
import certifi

train_data_path = 'data/train.csv'
test_data_path = 'data/test.csv'

train = pd.read_csv(train_data_path)
test = pd.read_csv(test_data_path)

CHUNKSIZE=100
index_name_train = "loan_prediction_train"
doc_type_train = "av-lp_train"

index_name_test = "loan_prediction_test"
doc_type_test = "av-lp_test"


# request_body = {
# 	"settings" : {
# 		"number_of_shards": 5,
# 		"number_of_replicas": 1
# 	},

# 	'mappings': {
# 		'examplecase': {
# 			'properties': {
# 				'Loan_ID': {'index': 'not_analyzed', 'type': 'string'},
# 				'Gender': {'index': 'not_analyzed', 'type': 'string'},
# 				'Married': {'index': 'not_analyzed', 'type': 'string'},
# 				'Dependents': {'index': 'not_analyzed', 'type': 'number'},
# 				'Education': {'index': 'not_analyzed', 'type': 'string'},
# 				'Self_Employed': {'index': 'not_analyzed', 'type': 'string'},
# 				'ApplicantIncome': {'index': 'not_analyzed', 'type': 'number'},
# 				'CoapplicantIncome': {'index': 'not_analyzed', 'type': 'number'},
# 				'LoanAmount': {'index': 'not_analyzed', 'type': 'number'},
# 				'Loan_Amount_Term': {'index': 'not_analyzed', 'type': 'number'},
# 				'Credit_History': {'index': 'not_analyzed', 'type': 'number'},
# 				'Property_Area': {'index': 'not_analyzed', 'type': 'string'},
# 				'Loan_Status': {'index': 'not_analyzed', 'type': 'string'}
# 			}
# 		}
# 	}
# }

def index_data(data_path, chunksize, index_name, doc_type):
	file = open(data_path)
	csvfile = pd.read_csv(file, iterator=True, chunksize=chunksize)

	es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

	try:
		es.indices.delete(index_name)
	except:
		pass

	es.indices.create(index=index_name)

	for i,df in enumerate(csvfile):
		records = df.where(pd.notnull(df), None).T.to_dict()
		# list_records = list(records.values())
		list_records = []

		for it in records:
			op_dict = {
				"index": {
					"_index": index_name,
					"_type": doc_type,
					"_id": it
				}
			}

			list_records.append(op_dict)
			list_records.append(records[it])

	try:
		es.bulk(index=index_name, body=list_records)
	except:
		print("Skipping a chunk")
		pass


index_data(train_data_path, CHUNKSIZE, index_name_train, doc_type_train)
index_data(test_data_path, CHUNKSIZE, index_name_test, doc_type_test)


print("DONE!!")