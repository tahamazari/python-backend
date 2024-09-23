import pandas as pd
from pysqldf import SQLDF, load_meat, load_births

sqldf = SQLDF(globals())

# Sample DataFrames
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie']
})

# Sample SQL Query
query = "SELECT * FROM df WHERE id > 1"

# Execute SQL Query
result = sqldf.execute(query)
print(result)