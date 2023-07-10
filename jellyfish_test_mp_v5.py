import pandas as pd
import jellyfish
from fuzzywuzzy import fuzz
from multiprocessing import Pool
import timeit
from datetime import datetime
from tqdm import tqdm

date_time = datetime.now()

# Load the data into a DataFrame
columns_to_read = ["item_no", "item_description", "vendor_item_no", "dw_evi_bu", "total_item_cost", "total_item_qty_on_hand"]
df = pd.read_parquet("files/Item_Statistics_Report.parquet", columns=columns_to_read)
print(df)
df = df[df["vendor_item_no"] != ""]
df_itemmaster = df[df['dw_evi_bu'] == "ITEMMASTER"]
print(df_itemmaster)
df = df[(df["total_item_cost"] != 0.0) & (df["total_item_qty_on_hand"] != 0)]
df = pd.concat([df, df_itemmaster])
print(df)
# df = df.sample(10000)
df = df.reset_index(drop=True)
print(df)
print(df.columns)

# Define a threshold for similarity
vendor_similarity_threshold = 0.98
description_similarity_threshold = 0.85

def export_matches(df):
    df.to_csv(f'files/jaro_winkler_matches_{date_time.strftime("%m%d%y%H%M%S")}.csv.gz',compression="gzip", index=False)
    df.to_parquet(f'files/jaro_winkler_matches_{date_time.strftime("%m%d%y%H%M%S")}.parquet', index=False)
    df.to_excel(f'files/jaro_winkler_matches_{date_time.strftime("%m%d%y%H%M%S")}.xlsx', index=False)

# Function to compare two strings and return similarity score
def compare_strings(i):
    item_no_1 = df.loc[i, 'item_no']
    item_description_1 = df.loc[i, 'item_description']
    vendor_id_1 = df.loc[i, 'vendor_item_no']
    dw_evi_bu_1 = df.loc[i, 'dw_evi_bu']
    
    similar_records = []
    for j in range(i+1, len(df)):
        item_no_2 = df.loc[j, 'item_no']
        item_description_2 = df.loc[j, 'item_description']
        vendor_id_2 = df.loc[j, 'vendor_item_no']
        dw_evi_bu_2 = df.loc[j, 'dw_evi_bu']
        if((dw_evi_bu_1 != "ITEMMASTER") or (dw_evi_bu_2 != "ITEMMASTER")):
            description_similarity = jellyfish.jaro_winkler(item_description_1, item_description_2)
            vendor_id_similarity = jellyfish.jaro_winkler(vendor_id_1, vendor_id_2)
            
            if description_similarity >= description_similarity_threshold and vendor_id_similarity >= vendor_similarity_threshold:
                similar_records.append((item_no_1, item_description_1, vendor_id_1, dw_evi_bu_1, item_no_2, item_description_2, vendor_id_2, dw_evi_bu_2, description_similarity, vendor_id_similarity))
    
    return similar_records

def main():
    # Define the number of processes for parallelization
    num_processes = 16

    # Perform string similarity comparisons in parallel
    # Perform string similarity comparisons in parallel with tqdm progress bar
    with Pool(num_processes) as p:
        results = []
        for result in tqdm(p.imap_unordered(compare_strings, range(len(df))), total=len(df)):
            if result:
                results.extend(result)

    # Flatten the list of similar records
    # similar_records = [record for sublist in similar_records for record in sublist]

    # Create a DataFrame from the matches
    matches_df = pd.DataFrame(results, columns=["item_no_1", "item_description_1", "vendor_item_no_1", "dw_evi_bu_1", "item_no_2", "item_description_2", "vendor_item_no_2", "dw_evi_bu_2","description_similarity", "vendor_similarity"])
    print(matches_df)
    export_matches(matches_df)


    # Display the similar records
    # for record in similar_records:
    #     print(f"Similar records found:\nRecord 1: {record[0]}, {record[2]}\nRecord 2: {record[1]}, {record[3]}\n")


if __name__ == '__main__':
    start = timeit.default_timer()
    main()
    end = timeit.default_timer()
    print(f'Duration: {end-start} secs')
    print(f'Duration: {(end-start)/60} mins')