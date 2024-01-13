import asyncio
import time

# Function to check if a number is prime
def is_prime(n):
    if n < 2:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True

# Async function to find primes in a range
async def find_primes(start, end):
    primes = []
    for n in range(start, end + 1):
        if is_prime(n):
            primes.append(n)
        # Yield control to the event loop every 1000 numbers
        if n % 1000 == 0:
            await asyncio.sleep(0)
    return primes

# Function to divide the work into tasks and run them
async def main():
    num_start = 0
    num_end = 100000
    task_num = 10
    num_init_div = num_end // task_num

    tasks = []
    for _ in range(task_num):
        start = num_start + 1
        end = num_start + num_init_div  # Corrected this line
        tasks.append(find_primes(start, end))
        num_start += num_init_div

    results = await asyncio.gather(*tasks)
    return results

start_time = time.time()
results = asyncio.run(main())
print("Time taken:", time.time() - start_time)

# Print the result
total = 0
for i, primes in enumerate(results):
    print(f"Task {i+1}: found {len(primes)} primes")
    total += len(primes)

print(f"Total primes: {total}")

############################################################################################################################################################################
