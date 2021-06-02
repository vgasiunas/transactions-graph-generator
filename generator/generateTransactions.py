import csv
import threading
import os
from models.Transaction import Transaction
from random import choices
from .utils import writeBatch, log

transactionHeaders = ['id', 'source', 'target', 'date', 'time', 'amount', 'currency']


def __generateTransactions_count(src_node, tgt_node, tran_file, batch_size, label, trans_count):
	try:
		os.remove(tran_file)
	except OSError:
		pass

	total_transactions = 0

	with open(tran_file, 'a') as transactions:
		batch = []
		src_nodes_count = 0

		def _batch_generator(_batch_size):
			for ndx in range(0, trans_count, batch_size):
				_batch = [Transaction(src,tgt).toRow(transactionHeaders)
						  for src, tgt in  zip(choices(src_node, k = _batch_size),
											   choices(tgt_node, k = _batch_size))]
				yield _batch

		for _batch_result in _batch_generator(batch_size):
			total_transactions += len(_batch_result)
			writeBatch(transactions, _batch_result)
			log(label + ": generating transactions: " + str(
				total_transactions) + ", total transaction count: " + str(trans_count))

		log(label + ": TOTAL: generating transactions: " + str(total_transactions))


def __generateTransactions_edges(edges, transactionsFile, batchSize, label):
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


def generateTransaction_edges(files, batchSize):
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

	clientSourcingTransactions = threading.Thread(target=lambda: __generateTransactions_edges(
		clientEdges,
		files['clients-sourcing-transactions'],
		batchSize,
		label='transaction(client->*)'
	))

	companyClientTransactions = threading.Thread(target=lambda: __generateTransactions_edges(
		companyEdges,
		files['companies-sourcing-transactions'],
		batchSize,
		label='transaction(company->client)'
	))

	clientSourcingTransactions.start()
	companyClientTransactions.start()

	clientSourcingTransactions.join()
	companyClientTransactions.join()


def generateTransactions_count(files, batchSize, trans_count):
	print("Reading nodes in memory")
	clients = list()
	companies = list()
	atms = list()

	with open(files['client'], 'r') as f:
		reader = csv.reader(f, delimiter="|")
		next(reader)
		log("Loading clients...")
		for row in reader:
			clients.append(row[0])

	with open(files['company'], 'r') as f:
		reader = csv.reader(f, delimiter="|")
		next(reader)
		log("Loading companies...")
		for row in reader:
			companies.append(row[0])

	with open(files['atm'], 'r') as f:
		reader = csv.reader(f, delimiter="|")
		next(reader)
		log("Loading atms...")
		for row in reader:
			atms.append(row[0])

	total_nodes = len(clients) + len(companies)
	log("total nodes: " + str(total_nodes))
	log("trans_count: " + str(int(trans_count*int(len(clients)/total_nodes))))
	log(str(trans_count*(len(clients)/total_nodes)))

	clientSourcingTransactions = threading.Thread(target=lambda: __generateTransactions_count(
		clients,
		clients,
		files['client-client-transactions'],
		batchSize,
		label='transaction(client->client)',
		trans_count=int(trans_count*len(clients)/total_nodes)
	))

	companyClientTransactions = threading.Thread(target=lambda: __generateTransactions_count(
		companies,
		clients,
		files['company-client-transactions'],
		batchSize,
		label='transaction(company->client)',
		trans_count=int(trans_count*len(companies)/total_nodes/2)
	))

	clientCompanyTransactions = threading.Thread(target=lambda: __generateTransactions_count(
		clients,
		companies,
		files['client-company-transactions'],
		batchSize,
		label='transaction(client->company)',
		trans_count=int(trans_count*len(companies)/total_nodes/2)
	))

	clientSourcingTransactions.start()
	companyClientTransactions.start()
	clientCompanyTransactions.start()

	clientSourcingTransactions.join()
	companyClientTransactions.join()
	clientCompanyTransactions.join()


def generateTransactions(files, batchSize, trans_count):
	if trans_count == 0:
		generateTransaction_edges(files, batchSize)
	else:
		generateTransactions_count(files, batchSize, trans_count)
