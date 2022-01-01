import os
import datetime
import requests
from requests.auth import HTTPBasicAuth
from sku_conversions import sku_dict


class Store():
    """ 
    A class used to represent an e-commerce store.
    
    Attributes
    ----------
    store_name (str) : The name of the store
    key (str) : The shipping platform API key
    secret (str) : The shipping platform API password
    store_id (str) : The store identification number
    db_connection (MySQLConnection object) : The database connection

    Methods
    -------
    import_awaiting_shipment_orders() : Import new awaiting shipment orders from the store

    parse_awaiting_shipment_order_data() : Extract relevant data from the store's awaiting shipment orders

    clean_and_normalize_order_data() : Clean and normalize awaiting shipment orders data

    create_pick_list() : Construct and write the store's pick list

    customers_with_multiple_orders() : Flag and write customers with multiple orders

    orders_containing_an_item_having_a_quantity_greater_than_one() : Flag and write orders containing an iten having a quantity greater than one

    awaiting_shipment_order_log() : Construct and write the record of awaiting shipment orders data
    """
    def __init__(self, store_name, key, secret, store_id, db_connection):
        """
        Parameters:
            store_name (str) : The name of the store
            key (str) : The shipping platform API key
            secret (str) : The shipping platform API password
            store_id (str) : The store identification number
            db_connection (MySQLConnection object) : The database connection
        """
        self.store_name = store_name
        self.key = key
        self.secret = secret
        self.store_id = store_id
        self.cnx = db_connection

        # list of dictionaries containing awaiting shipment orders metadata (not yet cleaned and normalized).
        self.awaiting_shipment_orders_list = []

        # dictionary containing awaiting shipment items.
        # Ex: { 'PREM-122': ['XL-1', 'MED-2', 'LRG-1'], 'PremTee-524': '5XL' }
        # key : (str)
        # value : (list(str)) or (str)
        self.skus_grouped_by_style_to_size_and_quantity = {}

        # list of dictionaries containing previously extracted awaiting shipment orders data.
        self.log = []


    def import_awaiting_shipment_orders(self):
        """
        Refresh and import the store's awaiting shipment orders data.

        Request awaiting shipment order metadata and set it to the instance variable awaiting_shipment_orders_list (list).
        """
        awaiting_shipment_response = requests.get(f'https://ssapi.shipstation.com/orders?orderStatus=awaiting_shipment&storeId={self.store_id}&sortBy=OrderDate&sortDir=DESC&pageSize=500', auth=HTTPBasicAuth(self.key, self.secret))

        awaiting_shipment_orders = awaiting_shipment_response.json()['orders']

        # list of dictionaries
        self.awaiting_shipment_orders_list = awaiting_shipment_orders


    def parse_awaiting_shipment_order_data(self):
        """
        Parse and extract awaiting shipment order data and insert into database tables.

        Parse instance variable awaiting_shipment_orders_list (list) and insert new order data and new item data into their respective database tables. Additionally, populate the log with all awaiting shipment orders data (new and old orders) for reference and record keeping.
        """
        for order in self.awaiting_shipment_orders_list:
            # dictionary for the current order's data. 
            log_dict = {}

            # Database iterator.
            cursor = self.cnx.cursor()

            order_num = order['orderNumber']
            cust_name = order['billTo']['name']

            # order_date is split to cut off milliseconds.
            order_date = order['orderDate'].split('.')[0]  # YYYY-MM-DDThh:mm:ss
            date, time = order_date.split('T')             # YYYY-MM-DD, hh:mm:ss
            year, month, day = date.split('-')             # YYYY, MM, DD
            hour, minute, second = time.split(':')         # hh, mm, ss

            # MySQL datetime: YYYY-MM-DD hh:mm:ss
            order_datetime = datetime.datetime(
                year=int(year),
                month=int(month),
                day=int(day),
                hour=int(hour),
                minute=int(minute),
                second=int(second)
                )

            # Log current order's order number, customer name, and date of transaction.
            log_dict['order_number'] = order_num
            log_dict['customer'] = cust_name
            log_dict['date'] = order_datetime

            # Check to see if this order is new; if it exists in the data table it is not new.
            select_query = f"SELECT * FROM {self.store_name}_order WHERE order_number = %s;"
            value = (order_num,) # value must be tuple or dictionary
            cursor.execute(select_query, value)

            # Fetch the row from the query, if the order is not new the row will contain data, else it will be an empty list.
            row = cursor.fetchone() # list

            # Not a new order -- order is in table.
            if row:
                # Set is_new flag in log to 0 (False).
                log_dict['is_new'] = 0

                # Set is_new flag in database table to 0 (False).
                update_query = f"UPDATE {self.store_name}_order SET is_new = 0 WHERE order_number = %s;"
                cursor.execute(update_query, value)
                self.cnx.commit()

                # A list of dictionaries with item information.
                items_list = order['items']

                # A list to contain the current order's items for the log (SKU and quantity).
                curr_order_items_list = []

                for item in items_list:
                    sku = item['sku']
                    name = item['name']         # Description of item that we include in each selling platform.
                    quantity = item['quantity'] # int

                    # Convert an obsolete SKU to revised SKU, if necessary.
                    if sku in sku_dict: sku = sku_dict[sku]

                    # If item does not have a SKU, assign its SKU as the item desciption.
                    if sku == '': sku = name

                    # Append item info as tuple.
                    curr_order_items_list.append((sku, quantity))

                # Set current order's items in log.
                log_dict['item_list'] = curr_order_items_list
            
            # New order -- extract info and add to table.
            else:
                # Set is_new flag in log to 1 (True).
                log_dict['is_new'] = 1

                # Insert order number, customer name, order date, and is_new Flag to True in database table.
                insert_order_query = f"""
                INSERT INTO {self.store_name}_order
                (order_number, customer_name, order_date, is_new)
                VALUES (%s, %s, %s, %s);
                """
                order_vals = (order_num, cust_name, order_datetime, 1)
                cursor.execute(insert_order_query, order_vals)
                self.cnx.commit()

                # A list of dictionaries with item information.
                items_list = order['items']

                # A list to contain the current order's items for the log (SKU and quantity).
                curr_order_items_list = []

                for item in items_list:
                    sku = item['sku']
                    name = item['name']         # Description of item that we include in each selling platform.
                    quantity = item['quantity'] # int

                    # Convert an obsolete SKU to revised SKU, if necessary.
                    if sku in sku_dict: sku = sku_dict[sku]

                    # If item does not have a SKU, assign its SKU as the item desciption.
                    if sku == '': sku = name

                    # Insert the item's order number, SKU, and quantity in database table.
                    insert_item_query = f"""
                    INSERT INTO {self.store_name}_item
                    (ord_num, sku, quantity)
                    VALUES (%s, %s, %s);
                    """
                    item_vals = (order_num, sku, quantity)
                    cursor.execute(insert_item_query, item_vals)
                    self.cnx.commit()

                    # Append item info as tuple.
                    curr_order_items_list.append((sku, quantity))

                # Set current order's items in log.
                log_dict['item_list'] = curr_order_items_list

            # Close database iterator.
            cursor.close()

            # Append current order's data to log.
            self.log.append(log_dict)


    def clean_and_normalize_order_data(self):
        """
        Clean and normalize awaiting shipment orders data.

        Query database tables for awaiting shipment orders data. Standardize SKU's of each item and populate the instance variable skus_grouped_by_style_to_size_and_quantity (dict) that will be used to create the pick list.
        """
        # Database iterator
        cursor = self.cnx.cursor()

        # Group NEW awaiting shipment items by SKU and their respective quantities.
        query = f"""
        SELECT sku, CAST(SUM(quantity) AS UNSIGNED)
        FROM {self.store_name}_item
        INNER JOIN {self.store_name}_order ON {self.store_name}_item.ord_num = {self.store_name}_order.order_number
        WHERE {self.store_name}_order.is_new = 1
        GROUP BY sku;
        """

        cursor.execute(query)

        # Extract each grouped item SKU and its quantity, then clean and insert data into skus_grouped_by_style_to_size_and_quantity (dict).
        for item in cursor.fetchall():
            SKU = item[0]               # str
            quantity = item[1]          # int
            sku_array = SKU.split('-')  # ex: ['<BRAND>', '<STYLE>', '<SIZE>']
            brand = sku_array[0]

            try:
                # Premiere Shirts
                if brand == 'PREM':
                    # SKU: three strings with '-' delimiter. 
                    # Ex: "PREM-823-MED", "PREM-150NEW-XL", "PREM-301P-5XL"
                    if len(sku_array) == 3:
                        premiere, style, size = SKU.split('-')
                        # Ex: style = "150NEW" or "301P"
                        if style[-3:] == 'NEW' or style[-1] == 'P':
                            style = style[:3]
                        premiere_and_style = premiere + '-' + style
                        # key : premiere_and_style (str), Ex: "PREM-823"
                        # value : [size-quantity] (list(str)), Ex: ["2XL-1", "MED-1", "LRG-2"]
                        if premiere_and_style not in self.skus_grouped_by_style_to_size_and_quantity:
                            self.skus_grouped_by_style_to_size_and_quantity[premiere_and_style] = [size + '-' + str(quantity)]
                        else:
                            self.skus_grouped_by_style_to_size_and_quantity[premiere_and_style].append(size + '-' + str(quantity))
                    # SKU: four strings with '-' delimiter.
                    # Ex: "PREM-812-RED-MED", "PREM-SS-153-LRG" -- (Inventory items that come in multiple colors / short sleeve).
                    elif len(sku_array) == 4:
                        premiere, index_1, index_2, size = SKU.split('-')
                        style = index_1 + '-' + index_2
                        premiere_and_style = premiere + '-' + style
                        # key : premiere_and_style (str), Ex: "PREM-823-RED"
                        # value : [size-quantity] (list(str)), Ex: ["2XL-1", "MED-1", "LRG-2"]
                        if premiere_and_style not in self.skus_grouped_by_style_to_size_and_quantity:
                            self.skus_grouped_by_style_to_size_and_quantity[premiere_and_style] = [size + '-' + str(quantity)]
                        else:
                            self.skus_grouped_by_style_to_size_and_quantity[premiere_and_style].append(size + '-' + str(quantity))
                # Premiere Jeans
                elif brand == 'PremJeans' or brand == 'PremiereJeans':
                    jean, color, size = SKU.split('-')
                    # Combine brands of jeans.
                    if jean == 'PremiereJeans':
                        jean = 'PremJeans'
                    jean_and_color = jean + '-' + color
                    # key : jean_and_color (str), Ex: "PremJeans-BLK"
                    # value : [size-quantity] (list(str)), Ex: ["32-1", "42-1", "36-3"]
                    if jean_and_color not in self.skus_grouped_by_style_to_size_and_quantity:
                        self.skus_grouped_by_style_to_size_and_quantity[jean_and_color] = [size + '-' + str(quantity)]
                    else:
                        self.skus_grouped_by_style_to_size_and_quantity[jean_and_color].append(size + '-' + str(quantity))
                # PremTee, PremiereLSTee (Non Dress Shirts)
                elif brand == 'PremTee' or brand == 'PremiereLSTee':
                    # SKU: three strings with '-' delimiter.
                    # Ex: "PremTee-524-XL", "PremiereLSTee-002-SML"
                    if len(sku_array) == 3:
                        shirt, style, size = SKU.split('-')
                        if size == 'XXL':
                            size = '2XL'
                        style_and_brand = style + '-' + shirt
                        # key : style_and_brand (str), Ex: "PremTee-524"
                        # value : [size-quantity] (list(str)), Ex: ["2XL-1", "MED-1", "LRG-2"]
                        if style_and_brand not in self.skus_grouped_by_style_to_size_and_quantity:
                            self.skus_grouped_by_style_to_size_and_quantity[style_and_brand] = [size + '-' + str(quantity)]
                        else:
                            self.skus_grouped_by_style_to_size_and_quantity[style_and_brand].append(size + '-' + str(quantity))
                    # SKU: four strings with '-' delimiter.
                    # Ex: "PremTee-NAVY-524-XL", "PremTee-WOM-002-SML", "PremiereLSTee-GRN-012-LRG" -- (Inventory items that come in multiple colors / women's styles).
                    elif len(sku_array) == 4:
                        shirt, index_1, index_2, size = SKU.split('-')
                        if size == 'XXL':
                            size = '2XL'
                        brand_style_color = shirt + '-' + index_2 + '-' + index_1
                        # key : style_and_brand (str), Ex: "PremiereLSTee-GRN-012-LRG"
                        # value : [size-quantity] (list(str)), Ex: ["2XL-1", "MED-1", "LRG-2"]
                        if brand_style_color not in self.skus_grouped_by_style_to_size_and_quantity:
                            self.skus_grouped_by_style_to_size_and_quantity[brand_style_color] = [size + '-' + str(quantity)]
                        else:
                            self.skus_grouped_by_style_to_size_and_quantity[brand_style_color].append(size + '-' + str(quantity))
                # All other brands
                else:
                    # key : SKU (str)
                    # value : quantity (int)
                    self.skus_grouped_by_style_to_size_and_quantity[SKU] = str(quantity)
            # An unanticipated SKU may appear from new or very old inventory, add to dictionary and print exception.
            except Exception as e:
                # key : SKU (str)
                # value : quantity (int)
                self.skus_grouped_by_style_to_size_and_quantity[SKU] = str(quantity)
                print(e)

        # Close database iterator.
        cursor.close()


    def create_pick_list(self):
        """
        Sort the previously cleaned and normalized awaiting shipment orders data by SKU (alphanumerically) and write to an external file as the pick list.

        Parse the instance variable skus_grouped_by_style_to_size_and_quantity (dict) and append each item's SKU and quantity to the pick_list (list). Sort the pick_list, then write each item to an external file. Include the item's quantity if more than one of the item is awaiting shipment, otherwise the quantity (1) will be omitted to signify a single item.
        """
        # Python's sort function sorts by ASCII. This dictionary acts as a custom comparator to sort items based on shirt size (SML, MED, LRG, XL, 2XL, 3XL, 4XL, 5XL, 6XL, 7XL, 8XL) and pant size (32, 34, 36, 38, 40, 42, 44, 46, 48, 50).
        desired_order = {'SM': 1, 'ME': 2, 'LR': 3, 'XL': 4, '2X': 5, '3X': 6, '4X': 7, '5X': 8, '6X': 9, '7X': 10, '8X': 11, '32': 12, '34': 13, '36': 14, '38': 15, '40': 16, '42': 17, '44': 18, '46': 19, '48': 20, '50': 21}

        pick_list = []

        for key, value in self.skus_grouped_by_style_to_size_and_quantity.items():
            # Only write the quantity if it is more than one.
            # value : [size-quantity] (list(str)), Ex: ["2XL-1", "MED-1", "LRG-2"]
            if type(value) is list:
                # Sort by desired_order (dict).
                value.sort(key=lambda x: desired_order[x[:2]])
                for index in range(len(value)):
                    # Ex: "4XL-1" -> size = "4XL", quant = "1"
                    size, quant = value[index].split('-')
                    # Keep the quantity of it is greater than one to show multiple items, otherwise it is unnecessary.
                    # Ex: "MED-1" -> "MED", "LRG-2" -> "LRG (2)""
                    if int(quant) == 1:
                        value[index] = size
                    else:
                        value[index] = size + ' (' + quant + ')'

                # "<style> -> " (Ex: "PREM-470 -> ")
                key = key + ' -> '

                # Write this item's sizes, all sizes comma separated if applicable.
                # Ex: "<style> -> MED, LRG, XL (2)"
                for index in range(len(value)):
                    if index + 1 == len(value):
                        key = key + str(value[index])
                    else:
                        key = key + str(value[index]) + ', '
                key = key + '\n'
            # value : quantity (str), represents the quantity of the item awaiting shipment, (Ex: "1", "2", etc).
            else:
                # Quantity of this item is greater than one.
                if int(value) > 1:
                    # Show quantity.
                    key = key + ' ... (' + value + ')' + '\n'
                # Quantity of this item is one.
                else:
                    # Do not show quantity of a single item.
                    key = key + '\n'

            # Add item to pick list.
            pick_list.append(key)

        pick_list.sort()

        # Write to an external file -- the pick list.
        with open(f'__ORDERS [{self.store_name}]__.txt', 'w', encoding='utf-8') as f:
            for item in pick_list:
                f.write(item)


    def customers_with_multiple_orders(self):
        """
        Flag all customers with multiple awaiting shipment orders.

        Query database tables to find any multiple new awaiting shipment orders purchased by the same customer. Write these customers and their respective order numbers to an external file. The file will be referenced and used to combine each customer's items for shipping.
        """
        # Database iterator
        cursor = self.cnx.cursor()

        # Temporary table created to hold a customer's name and their order quantity, only for current awaiting shipment orders.
        create_table_query = """
        CREATE TEMPORARY TABLE customer_with_multiple_order(
        customer_name CHAR(100) PRIMARY KEY,
        quantity INT
        );
        """
        cursor.execute(create_table_query)

        # Insert customer name and order quantity into table - only new awaiting shipment orders (is_new = True).
        insert_query = f"""
        INSERT INTO customer_with_multiple_order (customer_name, quantity)
        SELECT customer_name, CAST(COUNT(customer_name) AS UNSIGNED) as name_count
        FROM {self.store_name}_order
        WHERE {self.store_name}_order.is_new = 1
        GROUP BY customer_name
        HAVING name_count > 1;
        """
        cursor.execute(insert_query)

        # Select these customers and their order number.
        select_query = f"""
        SELECT customer_name, order_number
        FROM {self.store_name}_order
        WHERE customer_name IN (SELECT customer_name FROM customer_with_multiple_order);
        """
        cursor.execute(select_query)
        
        item_list = cursor.fetchall() # list

        # dictionary to hold each customer's name and their respective order numbers.
        order_info_dict = {}

        for row in item_list:
            customer = row[0]
            order_num = row[1]
            if customer not in order_info_dict:
                order_info_dict[customer] = [order_num]
            else:
                order_info_dict[customer].append(order_num)

        # Write each customer name, their order quantity, and each of their order numbers to an external file.
        with open(f'__ORDERS [{self.store_name}]__.txt', 'a', encoding='utf-8') as f:
            f.write('\nCUSTOMERS WITH MORE THAN ONE ORDER:\n\n')

            if order_info_dict:
                for key, value in order_info_dict.items():
                    f.write(key + ' - ')
                    f.write(str(len(value)) + ' Orders:\n')
                    for order_num in value:
                        f.write(order_num)
                    f.write('\n\n')
            else:   
                f.write('NO CUSTOMERS WITH MULTIPLE ORDERS')

        drop_query = "DROP TEMPORARY TABLE customer_with_multiple_order;"
        cursor.execute(drop_query)

        # Close database iterator.
        cursor.close()


    def orders_containing_an_item_having_a_quantity_greater_than_one(self):
        """
        Flag all awaiting shipment orders that contain an item having a quantity greater than one.

        Query database tables to find any new awaiting shipment orders that contain an item having a quantity greater than one. Write the order number, customer name, item, and its quantity to an external file. The file will be referenced and used for quality control, to verify the shipment contains all items for the customer's order.
        """
        # Database iterator
        cursor = self.cnx.cursor()

        # Temporary table created to hold an order number, customer's name, item, and its quantity, only for current awaiting shipment orders.
        create_table_query = """
        CREATE TEMPORARY TABLE order_with_multiple_quantity(
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_number CHAR(100),
        customer_name CHAR(100),
        sku CHAR(100),
        quantity INT
        );
        """
        cursor.execute(create_table_query)

        # Insert the order number, customer name, item, and its quantity into table - only new awaiting shipment orders (is_new = True).
        insert_query = f"""
        INSERT INTO order_with_multiple_quantity (order_number, customer_name, sku, quantity)
        SELECT O.order_number, O.customer_name, I.sku, I.quantity
        FROM {self.store_name}_item AS I
        INNER JOIN {self.store_name}_order AS O ON I.ord_num = O.order_number
        WHERE O.is_new = 1 AND I.quantity > 1;
        """
        cursor.execute(insert_query)

        select_query = "SELECT * FROM order_with_multiple_quantity;"
        cursor.execute(select_query)
        
        # Awaiting shipment orders containing an item having a quantity greater than one.
        item_list = cursor.fetchall() # list

        # Write each order number, customer name, item, and its quantity to an external file.
        with open(f'__ORDERS [{self.store_name}]__.txt', 'a', encoding='utf-8') as f:
            f.write('\nORDERS WITH MORE THAN ONE ITEM QUANTITY:\n\n')

            if item_list:
                for row in item_list:
                    order_num = row[1]
                    customer = row[2]
                    sku = row[3]
                    quantity = row[4]
                    f.write(order_num + ' - ' + customer + ' - ' + sku + ' (' + str(quantity) +')\n')
            else:
                f.write('NO ORDERS CONTAINING AN ITEM HAVING A QUANTITY GREATER THAN ONE')

        drop_query = "DROP TEMPORARY TABLE order_with_multiple_quantity;"
        cursor.execute(drop_query)

        #  Close database iterator.
        cursor.close()


    def awaiting_shipment_order_log(self):
        """
        Construct a record of all awaiting shipment orders data.

        Write all awaiting shipment orders data to an external file for reference and record keeping. Parse the instance variable log (list(dict)) and write each order's order number, customer name, date of transaction, order status (new or old), and items. 
        """
        # Create log, write the store name and current datetime.
        with open(f'_LOG-{self.store_name}_.txt', 'w', encoding='utf-8') as f:
            f.write(f'{self.store_name} LOG\n')
            # MM/DD/YYYY HH:MM:SS AM/PM
            f.write(datetime.datetime.now().strftime("%m/%d/%Y %I:%M:%S %p" + '\n\n'))

        # log : (list(dict))
        for order in self.log:
            order_number = order['order_number']
            customer = order['customer']
            date = order['date']
            is_new = order['is_new']
            # item_list : [(SKU, quantity)] (list(tuple(str, int))) 
            # Ex: [ ("PREM-100-SML", 1), ("PREM-833-MED", 1), ... etc ]
            item_list = order['item_list']

            # Write all (new and old) awaiting shipment orders to log.
            with open(f'_LOG-{self.store_name}_.txt', 'a', encoding='utf-8') as f:
                if not is_new:
                    f.write('=== NOT A NEW ORDER ===\n')
                f.write(order_number+'\n')
                f.write(customer+'\n')
                f.write(date.strftime("%m/%d/%Y %I:%M:%S")+'\n')
                for item in item_list:
                    sku = item[0]
                    quantity = item[1]
                    # Include the item quantity if greater than one.
                    if quantity > 1:
                        f.write(sku + ' (' + str(quantity) + ')' + '\n')
                    else:
                        f.write(sku + '\n')
                f.write('-'*30+'\n')