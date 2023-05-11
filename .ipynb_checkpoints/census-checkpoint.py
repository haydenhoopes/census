from json import loads
import numpy as np
import requests

class Census:
    base_url = "https://api.census.gov/data"
    
    def __init__(self, token):
        
        self.__api_data = loads(requests.get('https://api.census.gov/data.json').text)
        self.__token = token

    def info(self):
        print("This object facilitates the access of U.S. Census data.")

    
    # TOKEN
    def get_token(self):
        return self.__token
    
    def set_token(self, token):
        self.__token = token
        
    def get_token_str(self):
        return 'key=' + self.__token
    
    
    # API DATA
    def get_api_data(self):
        return self.__api_data
    
    
    
    
    def set_year(self, year, override_error=False):
        # if year not in self.__apis[self.get_api()]['datasets'][self.get_dataset()]['years'] and not override_error:
        #     years_available = self.__apis[self.get_api()]['datasets'][self.get_dataset()]['years']
        #     raise ValueError(f'Year not found for API {self.get_api()} and dataset {self.get_dataset()}. Years available are {years_available}')
        # else:
        self.__year = str(year)

    def get_year(self):
        return self.__year

    
    def set_api(self, api, override_error=False):
        if api not in self.__apis and not override_error:
            supported_apis = [api for api in self.__apis.keys()]
            raise ValueError(f'API value not found. Supported API\'s are {supported_apis}. Use `override_error=True` to silence error.')
        self.__api = api
    
    def get_api(self):
        try:
            return self.__api
        except AttributeError:
            raise AttributeError("Variable `api` not set")
    
    
    def set_dataset(self, dataset):
        # options are acs1, acsse, acs3, and acs5
        if dataset not in self.__apis[self.get_api()]['datasets'].keys():
            supported_datasets = [dataset for dataset in self.__apis[self.get_api()]['datasets'].keys()]
            raise ValueError(f'Dataset not found. Datasets for the {self.get_api()} API are {supported_datasets}')
        self.__dataset = dataset
        
    def get_dataset(self):
        try:
            return self.__dataset
        except AttributeError:
            raise AttributeError("Variable `dataset` not set")
    
    
    def set_table(self, table=None):
        # options are None/detail, subject, profile, cprofile, spp
        if table is not None and table not in self.__apis[self.get_api()]['datasets'][self.get_dataset()]['tables']:
            api = self.get_api()
            dataset = self.get_dataset()
            raise ValueError(f'Table not found. Tables for the {api} API and the {dataset} dataset include {self.__apis[api]["datasets"][dataset]["tables"]}')
        self.__table = table
        
    def get_table(self):
        try:
            return self.__table
        except AttributeError:
            raise AttributeError("Variable `table` not set")

    def get_table_str(self):
        return None if self.get_table() == 'detail' else self.get_table()
        
    
    def set_variables(self, **kwargs):
        variables = kwargs['variables']
        concepts  = kwargs['concepts']
        # variables.extend(self.get_concept_keys(concepts))
        self.__concepts = concepts
        self.__variables = variables
        
    def get_variables(self):
        try:
            all_variables = self.__variables.copy()
            all_variables.extend(self.pull_concept_keys(self.__concepts))
            return all_variables
        except AttributeError:
            raise AttributeError("Variable `variables` not set")

    def get_variables_str(self):
        variables = self.get_variables()
        if len(variables) == 0:
            raise ValueError('Must add one or more variables using `.set_variables()`')
        return 'get=' + ','.join(variables)

    def get_pull_variables_str(self, format='json'):
        year = self.get_year()
        api = self.get_api()
        dataset = self.get_dataset()
        return '/'.join([self.base_url, year, api, dataset, 'variables.' + format])

    def pull_variables_raw(self):
        return loads(requests.get(self.get_pull_variables_str()).text)

    def pull_all_variables(self):
        cols = []
        raw_data = self.pull_variables_raw()
        for variable in raw_data['variables']:
            for col in raw_data['variables'][variable]:
                if col not in cols:
                    cols.append(col)
        
        data = {}
        for col in cols:
            data[col] = []
        data['key'] = []

        for variable in raw_data['variables']:
            data['key'].append(variable)
            for col in cols:
                data[col].append(raw_data['variables'][variable].get(col, None))
        
        return data

    def pull_variable_concepts(self):
        return sorted(list(filter(None, set(self.pull_all_variables()['concept']))))

    def pull_concept_keys(self, *args):
        variables = self.pull_all_variables()
        concepts = np.array(variables['concept'])
        filt = np.isin(concepts, args)
        return np.array(variables['key'])[filt]
    
    def set_geography(self, **kwargs):
        # keys differ by dataset but can be (in descending hierarchy) us, region, division,
        # state, county, or city
        self.__geography = kwargs
        
    def get_geography(self):
        try:
            return self.__geography
        except AttributeError:
            raise AttributeError("Variable `geography` not set")
    
    def get_geography_str(self):
        geography = self.get_geography()
        if len(geography) == 0:
            raise ValueError('Must add one or more geographies using `.set_geography()`')
        geography_str = 'for='
        for key in geography:
            geography_str += f'{key}:{geography[key]}'
        return geography_str
    
    
    def create_api_table_str(self):
        return '/'.join([self.get_api(), self.get_dataset()]) if self.get_table_str() is None else '/'.join([self.get_api(), self.get_dataset(), self.get_table()])
    
    def create_request(self):
        uri = '/'.join([self.base_url, self.get_year(), self.create_api_table_str()])
        params = '&'.join([self.get_variables_str(), self.get_geography_str(), self.get_token_str()])
        return '?'.join([uri, params])

    
    def get_raw(self):
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
