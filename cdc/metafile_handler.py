import date_helper
class MetaFileHandler:

    # load id, column name, data typs from meta data file
    def __init__(self, meta_data):
        self.ids= meta_data.filter(lambda x: x.split(',')[-1]=='Y').map(lambda x:int(x.split(',')[0])).collect()
        self.names = meta_data.map(lambda x: [int(x.split(',')[0]), x.split(',')[1]]).collect()
        self.types = meta_data.map(lambda x: [int(x.split(',')[0]), x.split(',')[2]]).collect()

    # generate key-value
    def meta_kv_mapper(self, value):
        try:
            x = value.split(',')
            ids=[]
            for i in self.ids:
                ids.append(x[i-1])
            return tuple(ids), x
        except Exception as error:
            print error

    # compare column one by one
    def compare(self,x):
        is_same=True
        for n in range(0,len(x[0])):
            is_same = is_same and x[0][n]!=x[1][n]

        return not is_same

    # convert column to standard format base on its data type to make sure value is same
    def handle_data(self, x):
        for t in self.types:
            if t[1]=='date':
                time1 = date_helper.parse(x[1][t[0]-1]).strftime('%m-%d-%Y')
                x[1][t[0]-1]=time1

        return x


