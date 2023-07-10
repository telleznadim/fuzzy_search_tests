import pandas as pd
import jellyfish
from joblib import Parallel, delayed
from itertools import combinations
import timeit
from datetime import datetime
import multiprocessing

date_time = datetime.now()

# Assuming you have a DataFrame called 'df' with columns 'product_description' and 'seller_id'

def export_matches(df):
    df.to_csv(f'files/matches_{date_time.strftime("%m%d%y%H%M%S")}.csv.gz',compression="gzip", index=False)
    df.to_parquet(f'files/matches_{date_time.strftime("%m%d%y%H%M%S")}.parquet', index=False)
    df.to_excel(f'files/matches_{date_time.strftime("%m%d%y%H%M%S")}.xlsx', index=False)
    
def compare_products_wrapper(args):
    i, product1 = args[0]
    j, product2 = args[1]
    
    result = compare_products(product1, product2)
    return result

def compare_products(product1, product2):
    
    if((not product1['dw_evi_bu'] == "ITEMMASTER") and (not product2['dw_evi_bu'] == "ITEMMASTER")):
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
    else:
        return None
    
def main():
    columns_to_read = ["item_no", "item_description", "vendor_item_no", "dw_evi_bu"]
    df = pd.read_parquet("files/Item_Statistics_Report.parquet", columns=columns_to_read)
    df = df[df["vendor_item_no"] != ""]
    # df = df.sample(20000)
    print(df)
    print(df.columns)
    print("Building Combinations")

    # product_pairs = combinations(df.iterrows(), 2)
    product_pairs = list(combinations(df.iterrows(), 2))
    # print(product_pairs)
    # Create a multiprocessing pool
    pool = multiprocessing.Pool()


    print("Starting Comparing Process")
    # Map the compare_products function to the product pairs using multiprocessing
    matches = []
    for result in pool.imap_unordered(compare_products_wrapper, product_pairs):
        if result is not None:
            matches.append(result)
    print(matches)

    # Close the multiprocessing pool
    pool.close()
    pool.join()  # Ensure all processes are finished
    
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