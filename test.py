import pandas as pd
from fuzzysearch import find_near_matches

def fuzzy_search(query, text):
    matches = find_near_matches(query, text, max_l_dist=1)
    return [text[m.start:m.end] for m in matches]

# Example DataFrame
data = {'Name': ['John Smith', 'John Smith', 'John Smith', 'John Smith']}
df = pd.DataFrame(data)

# Compare each name with every other name
similarities = []
for i in range(len(df)):
    query = df.loc[i, 'Name']
    matches = []
    for j in range(i+1,len(df)):
        if i != j:
            name = df.loc[j, 'Name']
            name_matches = fuzzy_search(query, name)
            if name_matches:
                matches.append((name, name_matches))
    if matches:
        similarities.append((query, matches))

# Print the similarities
print(similarities)
for query, matches in similarities:
    print(f"Similarities found for '{query}':")
    for name, name_matches in matches:
        print(f" - With '{name}': {name_matches}")