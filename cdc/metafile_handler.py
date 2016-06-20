import date_helper
class MetaFileHandler:

	def __init__(self, meta_data):
		self.names = meta_data.map(lambda x: [int(x.split(',')[0]), x.split(',')[1]]).collect()
		self.types = meta_data.map(lambda x: [int(x.split(',')[0]), x.split(',')[2]]).collect()
		self.field_size=len(self.types)-1

	def meta_kv_mapper(self, value):
		try:
			x = value.split(',')
			return (x[0], x[2]), (x)
		except Exception as error:
			print value

	def compare(self,x):
		is_same=True
		for n in range(0,len(x[0])):
			is_same = is_same and x[0][n]!=x[1][n]

		return not is_same

	def handle_data(self, x):
		for t in self.types:
			if t[1]=='date':
				time1 = date_helper.parse(x[1][t[0]-1]).strftime('%m-%d-%Y')
				x[1][t[0]-1]=time1

		return x


