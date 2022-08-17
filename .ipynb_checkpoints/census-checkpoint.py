class Census:
    base_url = "https://api.census.gov/data"
    
    def __init__(self, token, year, dataset, table, *args, **kwargs):
        self.__token = token
        self.__year = str(year)
        self.__dataset = dataset
        self.__table = None if table == 'detail' else table
        self.__variables = args
        self.__geography = kwargs
        
        self.__apis = {
            # American Community Survey (ACS)
            'acs': [{'acs1'}, 'acsse', 'acs3', 'acs5']
            
        }
    
    def get_token(self):
        return self.__token
    
    def set_token(self, token):
        self.__token = token
        
    def get_token_str(self):
        return 'key=' + self.__token
    
    
    def set_year(self, year):
        self.__year = str(year)
        
    def get_year(self):
        return self.__year
    
    
    def set_dataset(self, dataset):
        # options are acs1, acsse, acs3, and acs5
        self.__dataset = dataset
        
    def get_dataset(self):
        return self.__dataset
    
    def get_dataset_str(self):
        if self.__dataset in self.__datasets['acs']:
            return f'acs/{self.__dataset}'
    
    
    def set_table(self, table):
        # options are None/detail, subject, profile, cprofile, spp
        self.__table = table
        
    def get_table(self):
        return self.__table
        
        
    def set_variables(self, *args):
        self.__variables = args
        
    def get_variables(self):
        return self.__variables
    
    def get_variables_str(self):
        if len(self.__variables) == 0:
            raise ValueError('Must add one or more variables using `.set_variables()`')
        return 'get=' + ','.join(self.__variables)
    
    def set_geography(self, **kwargs):
        # keys differ by dataset but can be (in descending hierarchy) us, region, division,
        # state, county, or city
        self.__geography == kwargs
        
    def get_geography(self):
        return self.__geography
    
    def get_geography_str(self):
        geography_str = 'for='
        for key in self.__geography:
            geography_str += f'{key}:{self.__geography[key]}'
        return geography_str
    
    
    def create_api_table_str(self):
        return self.get_dataset_str() if self.get_table() is None else '/'.join([self.get_dataset_str(), self.get_table()])
    
    def create_request(self):
        uri = '/'.join([self.base_url, self.get_year(), self.create_api_table_str()])
        params = '&'.join([self.get_variables_str(), self.get_geography_str(), self.get_token_str()])
        return '?'.join([uri, params])
    
    
    def get_raw(self):
        from json import loads
        return loads(requests.get(self.create_request()).text)
    
    def get(self):
        formatted_data = {}
        raw_data = self.get_raw()
        
        for col in enumerate(raw_data[0]):
            rows = []
            for row in raw_data[1:]:
                rows.append(row[col[0]])
            formatted_data[col[1]] = rows
            
        return formatted_data
            