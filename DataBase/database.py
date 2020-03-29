# BASE PATHS
base_path = "DataBase/Queries/{query}"

# Credentials
credential = open(base_path.format(query="credential.sql"), 'r').read()

# Descriptors
descriptors = open(base_path.format(query="descriptors.sql"), 'r').read()

# Mapper
dictionary = open(base_path.format(query="dictionary.sql"), 'r').read()
