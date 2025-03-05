import requests
import logging
import pandas as pd
from enum import Enum


logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

DATA = "data"
ATTRIBUTES = "attributes"
LEI = "lei"
ENTITY = "entity"
LEGAL_NAME = "legalName"
NAME = "name"
BIC_LIST = "bic"
LEGAL_ADDRESS = "legalAddress"
COUNTRY = "country"
NOTIONAL = "notional"
RATE = "rate"


# enums for identifying the country
class Country(Enum):
    NL = "NL"
    GB = "GB"


# empty cache initialized
cache = {}


def get_data(lei):
    # Check if the LEI is already cached
    if lei in cache:
        logger.info(f"Cache hit for LEI: {lei}")
        return cache[lei]

    # If not cached, make an API call
    url = f"{base_url}%5Blei%5D={lei}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        # Cache the result if the request was successful
        if response.status_code == 200:
            cache[lei] = response.json()  # Cache the response
            return cache[lei]
        else:
            logger.error(
                f"Failed API call for LEI {lei} with status code {response.status_code}"
            )
    except Exception as e:
        logger.error(f"API call failed for LEI {lei} with exception: {e}")
    return None


base_url = "https://api.gleif.org/api/v1/lei-records?filter"
try:
    df = pd.read_csv("/home/jaimol/cardano/files/input_dataset.csv")
    logger.info(f"CSV file loaded with {len(df)} rows")
except FileNotFoundError:
    logger.error("CSV file not found. Please check the file path.")
    exit(1)
except Exception as e:
    logger.error(f"Failed to load CSV file: {e}")
    exit(1)

required_columns = [LEI, NOTIONAL, RATE]
for col in required_columns:
    if col not in df.columns:
        raise KeyError(f"Missing required column in CSV: {col}")

for col in ["legal_name", "bic", "transaction_costs"]:
    if col not in df.columns:
        df[col] = None


output_csv_file = "gleif_data_output.csv"
output_data = []
for index, row in df.iterrows():
    lei = row["lei"]
    notional = row["notional"]
    rate = row["rate"]
    result = get_data(lei)
    if not result:
        logger.warning(f"Skipping row {index} due to missing data for LEI: {lei}")
        continue
    if DATA in result and len(result[DATA]) > 0:
        attributes = result[DATA][0].get(ATTRIBUTES, None)
        if attributes is None:
            logger.warning(f"Missing attributes for LEI: {lei}")
            continue

        lei_value = result[DATA][0][ATTRIBUTES].get(LEI, "")
        if lei_value:
            if lei != lei_value:
                logger.warning(f"LEI mismatch for {lei}. Skipping row.")
                continue
            bic_list = result[DATA][0][ATTRIBUTES].get(BIC_LIST, [])
            bic_str = ", ".join(bic_list) if isinstance(bic_list, list) else None
            entity = result[DATA][0][ATTRIBUTES].get(ENTITY, "")
            if entity:
                legal_name_dict = result[DATA][0][ATTRIBUTES][ENTITY].get(
                    LEGAL_NAME, {}
                )
                legal_name = (
                    legal_name_dict.get(NAME, "")
                    if isinstance(legal_name_dict, dict)
                    else ""
                )

                legal_address = result[DATA][0][ATTRIBUTES][ENTITY].get(
                    LEGAL_ADDRESS, {}
                )
                if rate == 0:
                    logger.warning(
                        f"Rate is zero for LEI {lei}. Skipping transaction costs calculation."
                    )
                    transaction_costs = None
                else:
                    country = legal_address.get(COUNTRY, "")
                    if country == Country.NL.value:
                        transaction_costs = abs(notional * (1 / rate) - notional)
                        logger.info(f"bic:{bic_str}")
                        logger.info(
                            f"Calculated transaction costs for LEI {lei} with {legal_name} (Country: NL): {transaction_costs}"
                        )

                    elif country == Country.GB.value:
                        transaction_costs = notional * rate - notional
                        logger.info(f"bic:{bic_str}")
                        logger.info(
                            f"Calculated transaction costs for LEI {lei} with {legal_name} (Country: GB): {transaction_costs}"
                        )
                    else:
                        transaction_costs = None
                        logger.warning(
                            f"Unsupported country for LEI {lei}. No transaction costs calculated."
                        )

                df.loc[index, "bic"] = bic_str
                df.loc[index, "legal_name"] = legal_name
                df.loc[index, "transaction_costs"] = transaction_costs

if __name__ == "__main__":
    df.to_csv(output_csv_file, index=False)
    logger.info(f"New CSV file '{output_csv_file}' created with API data merged.")
