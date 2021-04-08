import csv
import os
import threading
import math
import numpy as np
from random import random, sample
from .utils import writeBatch, log

def __transactionCount():
	y = random()
	return math.ceil((-10/4) * math.log(y))
	
def __generateEdges(sourceNodes, targetNodes, outputFile, connectionProbability, batchSize, label=''):
	try:
		os.remove(outputFile)
	except OSError:
		pass

	log(label + ": connProb: " + str(connectionProbability) )

	totalNumberOfEdges = 0
	with open(outputFile, 'a') as file:
		batch = []
		sourceNodesCount = 0

		def _batch_generator(batch_size):
			l = len(sourceNodes)
			for ndx in range(0, l, batch_size):
				_batch_src_nodes = sourceNodes[ndx:min(ndx + batch_size, l)]
				_batch = ['|'.join([src, '"' +
									str({tgt: __transactionCount() for tgt in sample(targetNodes,
												 max(int(len(targetNodes)
												 *np.random.normal(connectionProbability,0.005)),0))}
										  ) + '"'])
						  for src in _batch_src_nodes]


				yield _batch

		for _batch_result in _batch_generator(batchSize):
			sourceNodesCount += len(_batch_result)
			#edgesOutOfSourceNode = [sourceNode, {}]
			log(label + ': Source node number ' + str(sourceNodesCount) + ', number of edges: '
				+ str(totalNumberOfEdges))

			#totalNumberOfEdges += len(edgesOutOfSourceNode[1])
			#batch.append('|'.join(edgesOutOfSourceNode))

			writeBatch(file, _batch_result)
			batch = []
		#writeBatch(file, batch)
		log(label + ' ### TOTAL number of edges ' + str(totalNumberOfEdges))


def generateEdges(files, probs, batchSize):
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

	clientClientEdgesProcess = threading.Thread(target=lambda: __generateEdges(
		clients,
		clients,
		files['clients-clients-edges'],
		probs[0],
		batchSize,
		label='client->client'
	))
	clientCompanyEdgesProcess = threading.Thread(target=lambda: __generateEdges(
		clients,
		companies,
		files['clients-companies-edges'],
		probs[1],
		batchSize,
		label='client->company'
	))
	clientAtmEdgesProcess = threading.Thread(target=lambda: __generateEdges(
		clients,
		atms,
		files['clients-atms-edges'],
		probs[2],
		batchSize,
		label='client->atm'
	))
	companyClientEdgesProcess = threading.Thread(target=lambda: __generateEdges(
		companies,
		clients,
		files['companies-clients-edges'],
		probs[3],
		batchSize,
		label='company->client'
	))

	clientClientEdgesProcess.start()
	clientCompanyEdgesProcess.start()
	clientAtmEdgesProcess.start()
	companyClientEdgesProcess.start()

	clientClientEdgesProcess.join()
	clientCompanyEdgesProcess.join()
	clientAtmEdgesProcess.join()
	companyClientEdgesProcess.join()
