import asyncio
import aiohttp
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Function to fetch data from an API
async def fetch(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                return {"error": f"Failed to fetch from {url}, Status code: {response.status}"}
    except Exception as e:
        return {"error": str(e)}

# Wrapper to run asyncio in a thread for non-async operations
def run_async_tasks(urls):
    async def main():
        async with aiohttp.ClientSession() as session:
            tasks = [fetch(session, url) for url in urls]
            return await asyncio.gather(*tasks)

    return asyncio.run(main())

# Data processing with pandas
def process_data(data_list):
    # Convert data to a pandas DataFrame
    df = pd.DataFrame(data_list)
    # Example: Clean and filter the data
    df = df.dropna()  # Remove rows with missing values
    if 'error' in df.columns:
        df = df[df['error'].isna()]  # Remove error rows
    return df

# Example URLs for APIs
urls = [
    "https://jsonplaceholder.typicode.com/posts",
    "https://jsonplaceholder.typicode.com/users",
    "https://jsonplaceholder.typicode.com/todos"
]

if __name__ == "__main__":
    # Use ThreadPoolExecutor for CPU-bound tasks
    with ThreadPoolExecutor() as executor:
        # Fetch data concurrently using asyncio
        raw_data = run_async_tasks(urls)

        # Process data
        processed_data = process_data(raw_data)

        # Save processed data to CSV
        processed_data.to_csv("processed_data.csv", index=False)

    print("Data fetched, processed, and saved to 'processed_data.csv'.")
