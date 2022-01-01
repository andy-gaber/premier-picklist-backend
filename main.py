import config
import mysql.connector
import store
import refresh

# API Credentials
KEY = config.KEY
SECRET = config.SECRET

# Database Credendials
HOST = config.HOST
USER = config.USER
DB_PASSWORD = config.DB_PASSWORD
DATABSE = config.DATABASE

# Store ID's
AMAZON = config.AMAZON_STORE_ID
EBAY = config.EBAY_STORE_ID
ETSY = config.ETSY_STORE_ID
WEBSITE_ONE = config.WEBSITE_ONE_STORE_ID
WEBSITE_TWO = config.WEBSITE_TWO_STORE_ID
WEBSITE_THREE = config.WEBSITE_THREE_STORE_ID

# Suspend execution to allow enough time for all stores to import orders.
REFRESH_TIME = 60 # 60 seconds

STORES = [
('amazon', AMAZON),
('ebay', EBAY),
('etsy', ETSY),
('website_one', WEBSITE_ONE),
('website_two', WEBSITE_TWO),
('website_three', WEBSITE_THREE),
]

# Boolean
import_stores = refresh.refresh_stores(
    key=KEY,
    secret=SECRET,
    refresh_time=REFRESH_TIME,
    amazon_id=AMAZON,
    ebay_id=EBAY,
    etsy_id=ETSY,
    website_one_id=WEBSITE_ONE,
    website_two_id=WEBSITE_TWO,
    website_three_id=WEBSITE_THREE
    )

# If successfuly import all stores orders, run program.
if import_stores:
    try:
        cnx = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=DB_PASSWORD,
            database=DATABSE
            )
        
        for s in STORES:
            name = s[0]
            ID = s[1]

            current_store = store.Store(store_name=f'{name}', key=KEY, secret=SECRET, store_id=f'{ID}', db_connection=cnx)

            current_store.import_awaiting_shipment_orders()

            current_store.parse_awaiting_shipment_order_data()

            current_store.clean_and_normalize_order_data()

            current_store.create_pick_list()

            current_store.customers_with_multiple_orders()

            current_store.orders_containing_an_item_having_a_quantity_greater_than_one()

            current_store.awaiting_shipment_order_log()

        cnx.close()

    except mysql.connector.Error as e:
        print(e)

else:
    print('Error importing orders.')