import os
import heapq
import collections
import multiprocessing
import pandas as pd

# Function to read the log file (Excel format) and convert it into a list of rows
def read_log_file(filename):
    df = pd.read_excel(filename, engine='openpyxl', header=None)
    #df = pd.read_excel(filename)
    #print(df.head())
    return df[0].tolist()  # Convert column to list of rows

# Function to split the log file into smaller chunks based on a specific size
def split_file(lines, chunk_size=100_000, output_dir="chunks"):
    os.makedirs(output_dir, exist_ok=True)
    chunks = []
    for i in range(0, len(lines), chunk_size):
        chunk_file = os.path.join(output_dir, f'chunk_{i // chunk_size}.txt')
        with open(chunk_file, 'w', encoding='utf-8') as cf:
            cf.writelines("\n".join(lines[i:i + chunk_size]))
        chunks.append(chunk_file)
    return chunks


# Split by number of chunks and save each chunk to a file
# def split_file2(lines, num_chunks, output_dir="chunks"):
#     os.makedirs(output_dir, exist_ok=True)
#     chunk_size = len(lines) // num_chunks
#     chunk_files = []
#
#     for i in range(num_chunks):
#         start_index = i * chunk_size
#         end_index = (i + 1) * chunk_size if i < num_chunks - 1 else len(lines)
#         chunk_data = lines[start_index:end_index]
#
#         # Write the chunk to a file
#         chunk_file = os.path.join(output_dir, f'chunk_{i}.txt')
#         with open(chunk_file, 'w', encoding='utf-8') as cf:
#             cf.writelines("\n".join(chunk_data))
#
#         chunk_files.append(chunk_file)
#
#     return chunk_files


# # Split by number of chunks and store the data in lists - not memory and software efficient
# def split_file1(lines, num_chunks):
#     chunk_size = len(lines) // num_chunks
#     chunks1 = []
#     for i in range(num_chunks):
#         start_index = i * chunk_size
#         end_index = (i + 1) * chunk_size if i < num_chunks - 1 else len(lines)
#         chunks1.append(lines[start_index:end_index])
#     return chunks1

# Function to count error codes in a given chunk file
def count_errors(chunk_file):
    error_counter = collections.Counter()
    with open(chunk_file, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split("Error: ")
            if len(parts) > 1:
                error_code = parts[1].strip()
                error_counter[error_code] += 1
    return error_counter

# Main function to process the log file and find the top N most frequent error codes
def main(log_file, n, chunk_size=100_000):
    print("Reading the file...")
    lines = read_log_file(log_file)

    print("Splitting the file...")
    chunks = split_file(lines, chunk_size, output_dir="PART_1_A/chunks")
    #chunks = split_file1(lines, 10)
    #chunks = split_file2(lines, 107, "output_chunks")

    print("Counting error codes in each chunk...")
    with multiprocessing.Pool() as pool:
        counters = pool.map(count_errors, chunks)

    print("Merging the counts...")
    merged_counter = collections.Counter()
    for counter in counters:
        merged_counter.update(counter)

    print(f"Finding the top {n} most frequent error codes...")
    top_errors = heapq.nlargest(n, merged_counter.items(), key=lambda x: x[1])

    for error, count in top_errors:
        print(f"{error}: {count}")


if __name__ == "__main__":
    log_file = "PART_1_A/logs.txt.xlsx"
    n = int(input("Enter the value of n (the number of most frequent error codes you want to see): "))
    main(log_file, n)
