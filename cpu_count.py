import multiprocessing

num_cpus = multiprocessing.cpu_count()
print(f"Number of CPUs available: {num_cpus}")
