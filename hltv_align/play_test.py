import pandas as pd

# Create a sample DataFrame
data = {'Value': [4, 15, 8, 12, 7, 21, 3, 16, 10]}
df = pd.DataFrame(data)

# Define bin edges
bins = [0, 5, 10, 15, 20, 25]

# Create labels for the bins
labels = ['Bin 1', 'Bin 2', 'Bin 3', 'Bin 4', 'Bin 5']

# Use pandas.cut to bin the values
x = pd.cut(df['Value'], bins=bins)

# Display the DataFrame
print(x)