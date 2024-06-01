import os
import asyncio
import httpx
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from beanie import init_beanie, Document, Indexed, PydanticObjectId, DecimalAnnotation
from datetime import datetime
from dotenv import load_dotenv

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

# define a Pydantic model for fiat currency rates
class FiatRate(BaseModel):
    AED: DecimalAnnotation  # United Arab Emirates Dirham
    EUR: DecimalAnnotation  # Euro
    USD: DecimalAnnotation  # United States Dollar

# define a Pydantic model for cryptocurrency rates
class CryptoRate(BaseModel):
    BTC: DecimalAnnotation  # Bitcoin
    ETH: DecimalAnnotation  # Ethereum
    SOL: DecimalAnnotation  # Solana

# define a MongoDB document for fiat currency
class Fiat(Document):
    date: Indexed(datetime) = Field(default_factory=datetime.now) # the date retrieved, defaulting to the current date and time
    rates: FiatRate # rates for different fiat currencies

    class Settings:
        # collection name in MongoDB
        name = 'fiat'

# define a MongoDB document for cryptocurrency
class Crypto(Document):
    date: Indexed(datetime) = Field(default_factory=datetime.now) # the date retrieved, defaulting to the current date and time
    rates: CryptoRate # rates for different cryptocurrencies

    class Settings:
        # collection name in MongoDB
        name = 'crypto'

async def invert_exchange_rate(rate: dict) -> dict:
    '''
    convert the previous exchange rate to Ethiopian Birr (ETB)

    Parameters:
        rate: dict - the previous exchange rate dictionary

    Returns:
        dict: the inverted exchange rate
    '''
    # invert the exchange rates by dividing 1 by each rate
    return {key: str(1/DecimalAnnotation(value)) for key, value in rate.items()}

async def store_exchange_rate(inverted_rate: dict) -> None:
    '''
    store the exchange rates in MongoDB

    Parameters:
        inverted_rate: dict - the inverted exchange rate dictionary

    Returns:
        None
    '''
    try:
        # load the environment variable stored in the '.env' file from the current directory
        load_dotenv()
        # get the value of the 'MONGO' environment variable and save it in the 'MONGO' variable, which is our database connection string
        MONGO = os.getenv('MONGO')
        if not MONGO:
            # verify if the MONGO variable is not empty
            logging.critical('Environment variable Error: the MONGO variable is not set')
        # create an AsyncIOMotorClient instance with the MongoDB connection string
        client = AsyncIOMotorClient(MONGO)
        # initialize Beanie with the database and document models
        await init_beanie(database=client.birr, document_models=[Fiat, Crypto])
        # create a fiat document instance with the inverted exchange rate
        fiat = Fiat(rates=FiatRate(**inverted_rate))
        # insert the fiat document into the database
        await fiat.insert()
        # create a crypto document instance with the inverted exchange rate
        crypto = Crypto(rates=CryptoRate(**inverted_rate))
        # insert the crypto document into the database
        await crypto.insert()
    except Exception as e:
        # handle any exceptions associated with the database connection
        logging.critical(f'Database connection Error: {e}')

async def main() -> None:
    '''
    the main entry point of the program

    Parameters:
        None

    Returns:
        None
    '''
    # call the fetch_exchange_rate function and await its result
    rate = await fetch_exchange_rate()
    # call the invert_exchange_rate function and await its result
    inverted_rate = await invert_exchange_rate(rate)
    # call the store_exchange_rate function and await its result
    await store_exchange_rate(inverted_rate)

if __name__ == '__main__':
    # run the main function using the asyncio event loop
    asyncio.run(main())
