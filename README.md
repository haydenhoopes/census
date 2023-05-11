# census
 
To use this census file, simply import the Census object from the census.py file.

```python
from census import Census
```

Then, create a new Census object, passing in your token as it is instantiated:

```python
c = Census(token)
```

From here, the Census scraper needs to know a few things before it knows what data to scrape. First, it needs to know what API to use. You can see which APIs are available on the U.S. Census website, or in the code. Then, you need to set a database that is located in the selected API. Then, select a year that has available data in the selected database. Next, choose the table from the selected database.

```python
c.set_api('acs')
c.set_dataset('acs5')
c.set_year(2020)
c.set_table('detail')
```

Finally, you should set the variables that you want to get out of the table. The variables are tricky to understand and I wasn't able to finish cataloging them. However, they consist of concepts and variables, where a concept is a demographic of a specific group, and a variable is a specific attribute of the concept. Here is one that I got to work.

```python
c.set_variables(concepts=['AGE BY DISABILITY STATUS (AMERICAN INDIAN AND ALASKA NATIVE ALONE)', 'AGE BY DISABILITY STATUS BY POVERTY STATUS'], variables=['NAME', 'B19001B_012E', 'B24022_060M', 'B24022_060MA', 'B24022_060E'])
```

Finally, tell the scraper how to organize the rows. You'll have to specify if you want a specific state, county, or municipality and specify it by number (or just put a `*` to get all)

```python
c.set_geography(state='*')
```

Then, you can run the `.get()` method to pull down the data.

```python
c.get()
```

I also encourage you to use the other functionalities of the census scraper to learn more about concepts and variables. You can do this by using the `.pull_variable_concepts()` and `.pull_concept_keys()` methods. You can also use `.pull_all_variables()` to see data for all variables supported in the given data set that you are working with.

Let me know if there are any issues; I am happy to help answer them.
