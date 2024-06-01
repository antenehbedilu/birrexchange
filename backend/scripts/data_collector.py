import asyncio
import httpx
import logging

# configure logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filename='data_collector.log')

async def fetch_exchange_rate() -> dict:
    '''
    fetches the latest exchange rates from Coinbase API

    Parameters:
        None

    Returns:
        dict: includes exchange rates for fiat and cryptocurrencies
    '''
    # define Coinbase API URI
    base_url = 'https://api.coinbase.com/'
    endpoint = 'v2/exchange-rates?currency=ETB'
    # create an instance of the AsyncClient class from the httpx library
    async with httpx.AsyncClient() as client:
        try:
            # send a GET request to the Coinbase API
            response = await client.get(base_url + endpoint)
            # raise an exception for 4xx or 5xx status codes
            response.raise_for_status()
            # return the most recent exchange rates for Ethiopian Birr (ETB) in both fiat and cryptocurrencies
            return response.json()['data']['rates']
        except httpx.HTTPStatusError as e:
            # handle HTTP status errors (e.g. 404, 500)
            logging.error(f'Status Error: {e.response.status_code} - {e.response.reason_phrase}')
        except httpx.HTTPError as e:
            # handle HTTP errors (e.g. connection refused)
            logging.error(f'HTTP Exception: {e.request.url} - {e}')
        except httpx.RequestError as e:
            # handle request errors (e.g. DNS resolution failed)
            logging.error(f'Request Error: {e}')

async def main() -> None:
    '''
    the main entry point of the program

    Parameters:
        None

    Returns:
        None
    '''
    # call the fetch_exchange_rate function and await its result
    await fetch_exchange_rate()

if __name__ == '__main__':
    # run the main function using the asyncio event loop
    asyncio.run(main())
