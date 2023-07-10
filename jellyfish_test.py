import pandas as pd
import jellyfish
from joblib import Parallel, delayed
from itertools import combinations
import timeit
from datetime import datetime

date_time = datetime.now()

# Assuming you have a DataFrame called 'df' with columns 'product_description' and 'seller_id'

def export_matches(df):
    df.to_csv(f'files/matches_{date_time.strftime("%m%d%y%H%M%S")}.csv.gz',compression="gzip", index=False)
    df.to_parquet(f'files/matches_{date_time.strftime("%m%d%y%H%M%S")}.parquet', index=False)
    df.to_excel(f'files/matches_{date_time.strftime("%m%d%y%H%M%S")}.xlsx', index=False)
    

def compare_products(product1, product2):
    # Calculate similarity scores based on product description and seller ID
    description_similarity = jellyfish.jaro_winkler(product1['item_description'], product2['item_description'])
    seller_similarity = jellyfish.jaro_winkler(product1['vendor_item_no'], product2['vendor_item_no'])
    
    # Define a threshold for similarity
    seller_similarity_threshold = 0.98
    description_similarity_threshold = 0.85
    
    # Check if both similarity scores meet the threshold
    if description_similarity >= description_similarity_threshold and seller_similarity >= seller_similarity_threshold:
        # print(product1, product2, description_similarity, seller_similarity)
        return product1.item_no, product1.item_description, product1.vendor_item_no, product1.dw_evi_bu, product2.item_no, product2.item_description, product2.vendor_item_no, product2.dw_evi_bu, description_similarity, seller_similarity
    else:
        return None
    
def main():
    df = pd.read_parquet("files/Item_Statistics_Report.parquet")
    df = df.sample(1000)
    print(df)
    
    print(df.columns)

    # Get unique pairs of products
    product_pairs = combinations(df.iterrows(), 2)

    # List to store the matches
    matches = []

    # Iterate over unique pairs and compare products
    # for (i, product1), (j, product2) in tqdm(product_pairs, total=len(df)*(len(df)-1)//2):
    for (i, product1), (j, product2) in product_pairs:
        match = compare_products(product1, product2)
        if match is not None:
            matches.append(match)


    # Create a DataFrame from the matches
    matches_df = pd.DataFrame(matches, columns=["item_no_1", "item_description_1", "vendor_item_no_1", "dw_evi_bu_1", "item_no_2", "item_description_2", "vendor_item_no_2", "dw_evi_bu_2","description_similarity", "seller_similarity"])


    # Print the matches
    print(matches_df)
    if(not matches_df.empty):
        export_matches(matches_df)

if __name__ == '__main__':
    start = timeit.default_timer()
    main()
    end = timeit.default_timer()
    print(f'Duration: {end-start} secs')
    print(f'Duration: {(end-start)/60} mins')