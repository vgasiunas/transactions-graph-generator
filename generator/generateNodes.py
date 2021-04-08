import os
import threading
from models.Client import Client
from models.Company import Company
from models.ATM import ATM
from .utils import writeBatch, log

clientHeaders = ['id', 'first_name', 'last_name', 'age', 'email', 'occupation', 'political_views', 'nationality', 'university', 'academic_degree', 'address', 'postal_code', 'country', 'city']
companyHeaders = ['id', 'type', 'name', 'country']
atmHeaders = ['id', 'latitude', 'longitude']


def __generateModel(count, file, header, Model, modelname, batchSize, verbose=True):
	try:
		os.remove(file)
	except OSError:
		pass

	def _batch_generator(batch_size):
		_result = {Model() for c in range(batch_size)}
		return _result

	with open(file, 'a') as file:
		batch = []
		file.write('|'.join(header) + '\n')

		for i in range(count//batchSize):
			_batch = _batch_generator(batchSize)

			if verbose:
				log(str((i+1)*batchSize) + ' ' + modelname + ' of ' + str(count) + ' are generated')

			writeBatch(file, [e.toRow(header) for e in _batch])
			_batch = ""

		if count//batchSize*batchSize != count:
			_remainder = count-(count//batchSize*batchSize)
			_batch = _batch_generator(_remainder)
			writeBatch(file, [e.toRow(header) for e in _batch])
			_batch = ""

		log('TOTAL ' + modelname + ' of ' + str(count) + ' are generated')


def generateNodes(files, counts, batchSize):
	clientsProcess = threading.Thread(target=lambda : __generateModel(
		counts["client"],
		files["client"],
		header=clientHeaders,
		Model=Client,
		modelname='Client',
		batchSize=batchSize
	))
	companiesProcess = threading.Thread(target=lambda : __generateModel(
		counts["company"],
		files["company"],
		header=companyHeaders,
		Model=Company,
		modelname='Company',
		batchSize=batchSize
	))
	atmsProcess = threading.Thread(target=lambda : __generateModel(
		counts["atm"],
		files["atm"],
		header=atmHeaders,
		Model=ATM,
		modelname='ATM',
		batchSize=batchSize
	))

	clientsProcess.start()
	companiesProcess.start()
	atmsProcess.start()

	clientsProcess.join()
	companiesProcess.join()
	atmsProcess.join()
