import csv
import threading
import os
from models.Transaction import Transaction
from itertools import zip_longest
from .utils import writeBatch, log

transactionHeaders = ['id', 'source', 'target', 'date', 'time', 'amount', 'currency']

def __generateTransactions(edges, transactionsFile, batchSize, label):
	try:
		os.remove(transactionsFile)
	except OSError:
		pass

	totalNumberOfTransactions = 0

	with open(transactionsFile, 'a') as transactions:
		batch = []
		sourceNodesCount = 0

		def _batch_generator(batch_size):
			l = len(edges)
			log(label + ": total edges: " + str(l) + " BatchSize: " + str(batch_size))
			for ndx in range(0, l, batch_size):
				_b_edges =[(k,edges[k])
								   for k in list(edges.keys())[ndx:min(ndx + batch_size, l)]]

				_b_edges = [[(srcNode, _targets)
										for srcNode, _targets in
										zip([tran[0]]*len(tran[1]),tran[1].items())] for tran in _b_edges]

				_b_edges = [item for sublist in _b_edges for item in sublist]
				log(label + ": total targets: " + str(len(_b_edges)))

				_b_edges = [[Transaction(target[0],target[1][0]).toRow(transactionHeaders)
							 for _ in range(target[1][1])]
								for target in _b_edges]

				_b_edges = [item for sublist in _b_edges for item in sublist]

				yield _b_edges

		for _batch_result in _batch_generator(batchSize):
			sourceNodesCount += len(_batch_result)
			writeBatch(transactions, _batch_result)
			log(label + ": generating transactions for source node " + str(
				sourceNodesCount) + ", transaction count: " + str(totalNumberOfTransactions))

		log(label + ": TOTAL: generating transactions for source node " + str(sourceNodesCount) + ", transaction count: " + str(totalNumberOfTransactions))

def generateTransactions(files, batchSize):
	print("Reading nodes in memory")
	clientEdges = {}
	companyEdges = {}

	with open(files['clients-clients-edges'], 'r') as file:
		reader = csv.reader(file, delimiter="|")
		for row in reader:
			if not row[0] in clientEdges:
				clientEdges[row[0]] = {}
			clientEdges[row[0]].update(eval(row[1]))

	with open(files['clients-companies-edges'], 'r') as file:
		reader = csv.reader(file, delimiter="|")
		for row in reader:
			if not row[0] in clientEdges:
				clientEdges[row[0]] = {}
			clientEdges[row[0]].update(eval(row[1]))

	with open(files['clients-atms-edges'], 'r') as file:
		reader = csv.reader(file, delimiter="|")
		for row in reader:
			if not row[0] in clientEdges:
				clientEdges[row[0]] = {}
			clientEdges[row[0]].update(eval(row[1]))

	with open(files['companies-clients-edges'], 'r') as file:
		reader = csv.reader(file, delimiter="|")
		for row in reader:
			if not row[0] in clientEdges:
				companyEdges[row[0]] = {}
			companyEdges[row[0]].update(eval(row[1]))

	clientSourcingTransactions = threading.Thread(target = lambda: __generateTransactions(
		clientEdges,
		files['clients-sourcing-transactions'],
		batchSize,
		label='transaction(client->*)'
	))

	companyClientTransactions = threading.Thread(target = lambda: __generateTransactions(
		companyEdges,
		files['companies-sourcing-transactions'],
		batchSize,
		label='transaction(company->client)'
	))

	clientSourcingTransactions.start()
	companyClientTransactions.start()

	clientSourcingTransactions.join()
	companyClientTransactions.join()

