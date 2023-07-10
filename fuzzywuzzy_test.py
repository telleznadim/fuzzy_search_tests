import pandas as pd
from fuzzywuzzy import fuzz
from multiprocessing import Pool
import timeit
from datetime import datetime

date_time = datetime.now()

# Load the data into a DataFrame
columns_to_read = ["item_no", "item_description", "vendor_item_no", "dw_evi_bu", "total_item_cost", "total_item_qty_on_hand"]
df = pd.read_parquet("files/Item_Statistics_Report.parquet", columns=columns_to_read)
print(df)
df = df[df["vendor_item_no"] != ""]
df = df.sample(10000)
df = df.reset_index(drop=True)

# Define the similarity threshold (between 0 and 100)
vendor_similarity_threshold = 98
description_similarity_threshold = 85

def export_matches(df):
    df.to_csv(f'files/fuzzywuzzy_matches_{date_time.strftime("%m%d%y%H%M%S")}.csv.gz',compression="gzip", index=False)
    df.to_parquet(f'files/fuzzywuzzy_matches_{date_time.strftime("%m%d%y%H%M%S")}.parquet', index=False)
    df.to_excel(f'files/fuzzywuzzy_matches_{date_time.strftime("%m%d%y%H%M%S")}.xlsx', index=False)

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
        if((not dw_evi_bu_1 == "ITEMMASTER") and (not dw_evi_bu_2 == "ITEMMASTER")):
            description_similarity = fuzz.partial_ratio(item_description_1, item_description_2)
            vendor_id_similarity = fuzz.ratio(vendor_id_1, vendor_id_2)
            
            if description_similarity >= description_similarity_threshold and vendor_id_similarity >= vendor_similarity_threshold:
                similar_records.append((item_no_1, item_description_1, vendor_id_1, dw_evi_bu_1, item_no_2, item_description_2, vendor_id_2, dw_evi_bu_2, description_similarity, vendor_id_similarity))
    
    return similar_records

def main():
    # Define the number of processes for parallelization
    num_processes = 8

    # Perform string similarity comparisons in parallel
    with Pool(num_processes) as p:
        similar_records = p.map(compare_strings, range(len(df)))

    # Flatten the list of similar records
    similar_records = [record for sublist in similar_records for record in sublist]

    # Create a DataFrame from the matches
    matches_df = pd.DataFrame(similar_records, columns=["item_no_1", "item_description_1", "vendor_item_no_1", "dw_evi_bu_1", "item_no_2", "item_description_2", "vendor_item_no_2", "dw_evi_bu_2","description_similarity", "vendor_similarity"])
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