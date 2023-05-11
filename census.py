from json import loads
import requests
import pandas as pd
from wasabi import msg
import ast

class Census:
    base_url = "https://api.census.gov/data"
    
    def __init__(self, token):       
        ####  PRE LOAD INFORMATION  ####
        # API Data
        self.__api_raw_data = loads(requests.get('https://api.census.gov/data.json').text)
        self.__api_data = self.__api_raw_data['dataset']
        
        self.__api_options = {}

        for dataset in self.__api_data:
            if 'c_vintage' in dataset:
                year = str(dataset['c_vintage'])
                if year not in self.__api_options:
                    self.__api_options[year] = {}

                if len(dataset['c_dataset']) < 1:
                    raise ValueError(f'No api found for data set {dataset["title"]}')
                
                api = dataset['c_dataset'][0]
                if api not in self.__api_options[year].keys():
                    self.__api_options[year][api] = {}
                
                # Get database in API
                if len(dataset['c_dataset']) < 2:
                    pass
                else:
                    database = dataset['c_dataset'][1]
                    if database not in self.__api_options[year][api].keys():
                        self.__api_options[year][api][database] = []
                
                # Get table in database
                if len(dataset['c_dataset']) < 3:
                    pass
                else:
                    table = dataset['c_dataset'][2]
                    if table not in self.__api_options[year][api][database]:
                        self.__api_options[year][api][database].append(table)
    
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
    
    def get_api_options(self):
        return self.__api_options
    
    def pull_years(self):
        return self.__api_options.keys()
    
    def pull_apis(self):
        year = self.get_year()
        return self.__api_options[year].keys()
        
    def pull_databases(self):
        year = self.get_year()
        api = self.get_api()
        return self.__api_options[year][api].keys()
    
    def pull_tables(self):
        year = self.get_year()
        api = self.get_api()
        database = self.get_database()
        return self.__api_options[year][api][database]
    
    
    ### YEARS
    def set_year(self, year):
        try:
            del self.__api
            del self.__database
            del self.__table
            del self.__concepts
            del self.__variables
            del self.__geography
        except:
            pass

        if str(year) in self.pull_years():
            self.__year = str(year)
        else:
            raise ValueError(f'Not a valid year. Must be one of the following: {self.pull_years()}')

    def get_year(self):
        try:
            return self.__year
        except AttributeError:
            raise AttributeError(f"Variable `year` not set. Please set a year first. Supported years are {self.pull_years()}.")

    
    ### APIS
    def set_api(self, api):
        try:
            del self.__database
            del self.__table
            del self.__concepts
            del self.__variables
            del self.__geography
        except:
            pass

        if api in self.pull_apis():
            self.__api = api
        else:
            raise ValueError(f'API value not found. Supported API\'s for the year {self.get_year()} are {self.pull_apis()}.')
    
    def get_api(self):
        try:
            return self.__api
        except AttributeError:
            raise AttributeError(f"Variable `api` not set. Please pick an API first. Supported APIs for the year {self.get_year()} are {self.pull_apis()}.")
    

    ### DATABASES
    def set_database(self, database):
        try:
            del self.__table
            del self.__concepts
            del self.__variables
            del self.__geography
        except:
            pass

        if database in self.pull_databases():
            self.__database = database
        else:
            raise ValueError(f"Database value not valid. Supported databases for the year {self.get_year()} and the API {self.get_api()} are {self.pull_databases()}.")
        
    def get_database(self):
        try:
            return self.__database
        except AttributeError:
            raise AttributeError(f"Variable `dataset` not set. Please pick a database first. Supported databases for the year {self.get_year()} and the API {self.get_api()} are {self.pull_databases()}.")
    

    ### TABLES
    def set_table(self, table):
        try:
            del self.__concepts
            del self.__variables
            del self.__geography
        except:
            pass

        if table in self.pull_tables() or table == 'detail':
            self.__table = table
            if table == 'detail':
                msg.warn(f'Warning: The `detail` table may or may not exist in the database {self.get_database()}.')
        else:
            raise ValueError(f"Table not valid. Supported tables for the year {self.get_year()} and the API {self.get_api()} in the database {self.get_database()} are {self.pull_tables()}.")
        
    def get_table(self):
        try:
            return self.__table
        except AttributeError:
            raise AttributeError(f"Variable `table` not set. Please pick a table first. Supported tables for the year {self.get_year()} and the API {self.get_api()} in the database {self.get_database()} are {self.pull_tables()}.")
        
    
    def get_link(self, year, api, database, table, endpoint, extension=None, *args):
        if extension:
            if table == 'detail':            
                return self.base_url + '/' + '/'.join([year, api, database]) + f'/{endpoint}.{extension}'
            else:            
                return self.base_url + '/' + '/'.join([year, api, database, table]) + f'/{endpoint}.{extension}'


    ### CONCEPT
    # This function is a helper function
    def process_variables_and_concepts(self, year, api, database, table):
        variables = loads(requests.get(self.get_link(year, api, database, table, 'variables', 'json')).text)


        concepts = {}
        for variable in variables['variables'].keys():
            if 'concept' in variables['variables'][variable]:
                concept = variables['variables'][variable]['concept']
                if concept in concepts.keys():
                    pass
                else:
                    concepts[concept] = []

                new_variable = {'variable': variable}
                new_variable.update(variables['variables'][variable])
                del new_variable['concept']

                concepts[concept].append(new_variable)
        
        return concepts

    def pull_concepts(self):
        year = self.get_year()
        api = self.get_api()
        database = self.get_database()
        table = self.get_table()
        return self.process_variables_and_concepts(year, api, database, table).keys()
    
    def set_concepts(self, *args):
        try:
            del self.__variables
        except:
            pass

        msg.warn('New concepts set. Variables cleared.')
        concepts = []
        if args == []:
            raise ValueError('Blank concept not allowed.')
        for arg in args:
            if arg in self.pull_concepts():
                concepts.append(arg)
            else:
                raise ValueError(f"Concept {arg} not allowed. Supported concepts for the year {self.get_year()} and the API {self.get_api()} in the database {self.get_database()} and the {self.get_table()} are {self.pull_concepts()}. ")
        self.__concepts = concepts
    
    def get_concepts(self):
        try:
            return self.__concepts
        except AttributeError:
            raise AttributeError(f'Variable `concepts` not set. Supported concepts for the year {self.get_year()} and the API {self.get_api()} in the database {self.get_database()} and the {self.get_table()} are {self.pull_concepts()}.')

    def pull_concepts_and_variables(self, as_dataframe=False):
        year = self.get_year()
        database = self.get_database()
        api = self.get_api()
        table = self.get_table()
        data = self.process_variables_and_concepts(year, api, database, table)
        if not as_dataframe:
            return data
        else:
            # Get column names
            columns = ['concept']
            for key in data.keys():
                for variable in data[key]:
                    for v_key in variable.keys():
                        if v_key not in columns:
                            columns.append(v_key)
            dataframe = {}
            for column in columns:
                dataframe[column] = []
            
            for key in data.keys():
                for variable in data[key]:
                    dataframe['concept'].append(key)
                    for v_key in variable.keys():
                        dataframe[v_key].append(variable[v_key])
                        max_col_len = len(dataframe[v_key])
                    for col in dataframe.keys():
                        if len(dataframe[col]) < max_col_len:
                            dataframe[col].append(None)
            return pd.DataFrame(dataframe)
    

    ### VARIABLES
    def pull_variables(self, as_list=False, as_dataframe=False):
        try:
            if as_list and as_dataframe:
                raise ValueError('Cannot return list and dataframe simultaneously.')
            
            concepts = self.get_concepts()
            if as_list:
                all_concepts = self.pull_concepts_and_variables(as_dataframe=True)
                return list(all_concepts[all_concepts['concept'].isin(concepts)]['variable'])
            elif as_dataframe:
                all_concepts = self.pull_concepts_and_variables(as_dataframe=True)
                return all_concepts[all_concepts['concept'].isin(concepts)]
            else:
                all_concepts = self.pull_concepts_and_variables()
                return {key: all_concepts[key] for key in concepts if key in all_concepts}         
        except AttributeError:
            msg.warn('Warning: Variable `concept` not set. Returning all variables.')
            if as_list:
                all_concepts = self.pull_concepts_and_variables(as_dataframe=True)
                return list(all_concepts[all_concepts['concept'].isin(concepts)]['variable'])            
            else:
                return self.pull_concepts_and_variables(as_dataframe)

    def clear_variables(self):
        self.__variables = None

    def add_variable(self, variable):
        try:
            if variable in self.pull_variables(as_list=True) or variable == 'NAME':
                if variable in self.get_variables():
                    raise ValueError(f'Variable {variable} already exists.')
                self.__variables.append(variable)
            else:
                raise ValueError(f'Variable {variable} not allowed.')
        except:
            self.__variables = []
            if variable in self.pull_variables(as_list=True):
                self.__variables.append(variable)
            else:
                raise ValueError(f'Variable {variable} not allowed.')

    def set_variables(self, *args):
        for arg in args:
            self.add_variable(arg)

    def get_variables(self):
        try:
            return self.__variables
        except AttributeError:
            raise AttributeError(f'Variable `variables` not set. Use the `pull_variables()` method to check which variables are available.')

        
    
    ### GEOGRAPHY
    def set_geography(self, geography):
        self.__geography_values = None

        if geography in self.pull_geographies(as_list=True):
            for g in self.pull_geographies():
                if g['name'] == geography and 'requires' in g:
                    msg.warn(f"Geography {geography} requires {g['requires']}. Add additional details by using the `set_geography()` method.")
            self.__geography = geography
            self.__geography_values = ['*']
            msg.warn(f'Using * to get all of geography {geography}. Use the `set_geography_values()` method to specify.')
        else:
            raise AttributeError(f"Geography {geography} not allowed. Supported geographies for year {self.get_year()} and API {self.get_api()} and database {self.get_database()} and table {self.get_table()} are {self.process_geographies(self.pull_geographies())}.")

    def pull_geographies(self, as_list=False):
        year = self.get_year()
        api = self.get_api()
        database = self.get_database()
        table = self.get_table()
        geographies = loads(requests.get(self.get_link(year, api, database, table, 'geography', 'json')).text)['fips']
        if as_list:
            return [g['name'] for g in geographies]
        else:
            return geographies
    
    def get_geography(self):
        try:
            self.__geography
            return self.__geography
        except AttributeError:
            raise AttributeError(f"Variable `geography` not set. Supported geographies for year {self.get_year()} and API {self.get_api()} and database {self.get_database()} and table {self.get_table()} are {self.pull_geographies(as_list=True)}.")
    
    def pull_geography_values(self, as_dataframe=False):
        year = self.get_year()
        api = self.get_api()
        database = self.get_database()
        table = self.get_table()
        # variables = self.get_variables()
        geography = self.get_geography()

        if table == 'detail':
            r = requests.get(self.base_url + '/' + '/'.join([year, api, database]) + '?get=NAME&for=' + geography + ':*').text
        else:
            r = ast.literal_eval(requests.get(self.base_url + '/' + '/'.join([year, api, database, table]) + '?get=NAME&for=' + geography + ':*').text)
        if as_dataframe:
            return pd.DataFrame(r, columns=r[0]).drop(0, axis=0)
        else:
            return r
        
    def add_geography_value(self, value):
        # Check if geography value is okay
        if value in list(self.pull_geography_values(as_dataframe=True).iloc[:, 1]):
            value = str(value)
            try:
                if '*' in self.__geography_values:
                    self.__geography_values = [value]
                else:
                    self.__geography_values.append(value)
            except:
                self.__geography_values = [value]
        else:
            raise AttributeError(f"Value {value} not allowed for this geography {self.get_geography()}")
        
    def get_geography_values(self):
        return self.__geography_values


    def label_data(self, df):
        # Get variable labels
        labels = {}
        all_variables = self.pull_variables(as_dataframe=True)
        for v in self.get_variables():
            labels[v] = list(all_variables.loc[all_variables['variable'] == v, 'label'])[0]
        
        df = df.rename(columns=labels)

        labels = {}
        all_geography_values = self.pull_geography_values(as_dataframe=True)

        for row in df[self.get_geography()]:
            labels[row] = list(all_geography_values.loc[all_geography_values[self.get_geography()] == row, 'NAME'])[0]
        
        df[self.get_geography()] = df[self.get_geography()].replace(labels)

        return df


    def pull_data(self, as_dataframe=False, labelled=False):
        year = self.get_year()
        api = self.get_api()
        database = self.get_database()
        table = self.get_table()
        variables = self.get_variables()
        geography = self.get_geography()
        geography_values = self.get_geography_values()


        if table == 'detail':
            url = self.base_url + '/' + '/'.join([year, api, database]) + '?get=' + ','.join(variables) + '&for=' + geography + ':' + ','.join(geography_values)
            print(url)
            r = requests.get(url).text
            return r
        else:
            url = self.base_url + '/' + '/'.join([year, api, database, table]) + '?get=' + ','.join(variables) + '&for=' + geography + ':' + ','.join(geography_values)
        print(url)
        r = ast.literal_eval(requests.get(url).text)
        if as_dataframe:
            if labelled:
                return self.label_data(pd.DataFrame(r, columns=r[0]).drop(0, axis=0))
            else:
                return pd.DataFrame(r, columns=r[0]).drop(0, axis=0)
        else:
            return r

