import time
import requests
from requests.auth import HTTPBasicAuth


def refresh_stores(key, secret, refresh_time, amazon_id, ebay_id, etsy_id, website_one_id, website_two_id, website_three_id):
    """
    Refresh and import orders from the selling platforms.

    Args:
        key (str) : The shipping platform API key
        secret (str) : The shipping platform API password
        refresh_time (int) : The time to suspend execution to allow all stores to import awaiting shipment orders
        amazon_id (str) : The Amazon store identification number
        ebay_id (str) : The eBay store identification number
        etsy_id (str) : The Etsy store identification number
        website_one_id (str) : The first website store identification number
        website_two_id (str) : The second website store identification number
        website_three_id (str) : The third website store identification number

    Returns:
        bool : True if store refresh was successful, else False
    """
    amazon_response = requests.post(f'https://ssapi.shipstation.com/stores/refreshstore?storeId={amazon_id}', auth=HTTPBasicAuth(key, secret))

    ebay_response = requests.post(f'https://ssapi.shipstation.com/stores/refreshstore?storeId={ebay_id}', auth=HTTPBasicAuth(key, secret))

    etsy_response = requests.post(f'https://ssapi.shipstation.com/stores/refreshstore?storeId={etsy_id}', auth=HTTPBasicAuth(key, secret))
    
    website_one_response = requests.post(f'https://ssapi.shipstation.com/stores/refreshstore?storeId={website_one_id}', auth=HTTPBasicAuth(key, secret))
    
    website_two_response = requests.post(f'https://ssapi.shipstation.com/stores/refreshstore?storeId={website_two_id}', auth=HTTPBasicAuth(key, secret))
    
    website_three_reponse = requests.post(f'https://ssapi.shipstation.com/stores/refreshstore?storeId={website_three_id}', auth=HTTPBasicAuth(key, secret))

    # If the request for each store refresh is successful suspend execution to allow enough time for all stores to import orders.
    try:
        if (amazon_response.json()['success'] == 'true' and 
            ebay_response.json()['success'] == 'true' and
            etsy_response.json()['success'] == 'true' and
            website_one_response.json()['success'] == 'true' and
            website_two_response.json()['success'] == 'true' and
            website_three_reponse.json()['success'] == 'true'):

            print('Refreshing stores and importing orders...\n')
            time.sleep(refresh_time)
            return True

    except Exception as e:
        print(e)
        return False