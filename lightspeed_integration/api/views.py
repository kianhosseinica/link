import requests
from django.http import JsonResponse
import logging
import time
from django.shortcuts import render
from .forms import SystemSkuForm

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants for API URLs and organization ID
ZOHO_API_BASE_URL = "https://www.zohoapis.com/books/v3"
ORGANIZATION_ID = "762023225"
ZOHO_RATE_LIMIT = 100  # requests per minute
zoho_access_token = None
request_count = 0
purchase_account_id = None  # To be set globally after fetching

# Function to refresh Lightspeed access token
def refresh_access_token():
    url = "https://cloud.lightspeedapp.com/oauth/access_token.php"
    payload = {
        "client_id": "6e9be2c0819d3e6e77213368de1a4b5308d94bae4a1698af014bbbbce71f4ccd",
        "client_secret": "07a118c07adedd5427bfd4c793410c5dc11472f79d6ad4854f04d00eadff48fa",
        "refresh_token": "84149496a4213a36bd3e7a5131cdaf1521167093",
        "grant_type": "refresh_token"
    }

    response = requests.post(url, data=payload)
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        logger.error(f"Failed to refresh Lightspeed access token: {response.text}")
        return None

# Function to fetch all items from Lightspeed
def get_all_items():
    access_token = refresh_access_token()
    if not access_token:
        return []

    headers = {"Authorization": f"Bearer {access_token}"}
    url = "https://api.lightspeedapp.com/API/V3/Account/292471/Item.json"
    items = []

    while url:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            items.extend([{
                "defaultCost": item.get("defaultCost"),
                "description": item.get("description"),
                "manufacturerSku": item.get("manufacturerSku"),
                "price": next((price.get("amount") for price in item.get("Prices", {}).get("ItemPrice", []) if price.get("useType") == "Default"), None)
            } for item in data.get('Item', [])])
            url = data['@attributes'].get('next')
        else:
            logger.error(f"Failed to fetch items from Lightspeed: {response.text}")
            return []

    return items

# Function to fetch item details from Lightspeed by manufacturerSku
def get_lightspeed_item_details(sku):
    access_token = refresh_access_token()
    if not access_token:
        return None

    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.lightspeedapp.com/API/V3/Account/292471/Item.json?manufacturerSku={sku}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        item = None
        if 'Item' in data:
            if isinstance(data['Item'], list) and len(data['Item']) > 0:
                item = data['Item'][0]
            elif isinstance(data['Item'], dict):
                item = data['Item']
        if item:
            return {
                "defaultCost": item.get("defaultCost"),
                "description": item.get("description"),
                "manufacturerSku": item.get("manufacturerSku"),
                "price": next((price.get("amount") for price in item.get("Prices", {}).get("ItemPrice", []) if price.get("useType") == "Default"), None)
            }
    logger.error(f"Failed to fetch item from Lightspeed for SKU {sku}: {response.text}")
    return None

# Function to refresh Zoho access token
def refresh_zoho_access_token():
    url = "https://accounts.zoho.com/oauth/v2/token"
    payload = {
        "refresh_token": "1000.7d421efc934f671f5d004dcc93c69cfe.cc9d8fb5b418c658fd0af28eb71f9530",
        "client_id": "1000.4EBWLV02KO1UA1L0YRUSYWVNYZYUQF",
        "client_secret": "dd42af92df0b3974f285a6e3b41d83a6891f80fe1b",
        "grant_type": "refresh_token",
        "redirect_uri": "http://localhost:8000/callback/"
    }

    response = requests.post(url, data=payload)
    if response.status_code == 200:
        global zoho_access_token
        zoho_access_token = response.json().get('access_token')
        return zoho_access_token
    else:
        logger.error(f"Failed to refresh Zoho access token: {response.text}")
        return None

# Function to get Zoho headers with a valid access token
def get_zoho_headers():
    global zoho_access_token
    if not zoho_access_token:
        zoho_access_token = refresh_zoho_access_token()

    headers = {
        "Authorization": f"Zoho-oauthtoken {zoho_access_token}",
        "Content-Type": "application/json"
    }
    return headers

# Function to manage API rate limits
def handle_rate_limit():
    global request_count
    request_count += 1
    if request_count >= ZOHO_RATE_LIMIT:
        logger.info("Rate limit reached, sleeping for 60 seconds...")
        time.sleep(60)
        request_count = 0

# Function to make a request with automatic token refresh
def make_zoho_request(method, url, headers, data=None):
    handle_rate_limit()
    response = requests.request(method, url, headers=headers, json=data)
    if response.status_code == 401:  # Token expired
        logger.info("Zoho access token expired. Refreshing token...")
        headers = get_zoho_headers()
        response = requests.request(method, url, headers=headers, json=data)
    return response

# Function to fetch all items from Zoho
def get_all_zoho_items():
    headers = get_zoho_headers()
    url = f"{ZOHO_API_BASE_URL}/items?organization_id={ORGANIZATION_ID}&filter_by=Status.Active"
    items = []

    while url:
        response = make_zoho_request("GET", url, headers)
        if response.status_code == 200:
            data = response.json()
            items.extend([{
                "item_id": item.get("item_id"),
                "name": item.get("name"),
                "rate": item.get("rate"),
                "purchase_rate": item.get("purchase_rate"),
                "sku": item.get("sku"),
            } for item in data['items']])
            page_context = data.get("page_context", {})
            url = f"{ZOHO_API_BASE_URL}/items?organization_id={ORGANIZATION_ID}&filter_by=Status.Active&page={page_context['page'] + 1}&per_page={page_context['per_page']}" if page_context.get("has_more_page") else None
        else:
            logger.error(f"Failed to fetch items from Zoho: {response.text}")
            return []

    return items

# Function to check if an item exists in Zoho
def check_item_exists_in_zoho(sku, zoho_items):
    return next((item for item in zoho_items if item['sku'] == sku), None)

# Function to normalize values for comparison
def normalize_value(value):
    if value is None or value == "":
        return ''
    if isinstance(value, str):
        return value.strip().lower()
    if isinstance(value, float):
        return round(value, 2)
    return value

def compare_floats(value1, value2, tolerance=0.01):
    try:
        float1 = float(value1)
        float2 = float(value2)
        return abs(float1 - float2) <= tolerance
    except (TypeError, ValueError) as e:
        logger.error(f"Error comparing floats: {e}. Values: {value1}, {value2}")
        return False

# Function to update items in Zoho
def update_item_in_zoho(item_id, fields, headers):
    max_retries = 3
    retry_delay = 2  # seconds

    for attempt in range(max_retries):
        response = make_zoho_request("PUT", f"{ZOHO_API_BASE_URL}/items/{item_id}?organization_id={ORGANIZATION_ID}", headers, fields)
        if response.status_code == 200:
            logger.info(f"Successfully updated item in Zoho: {item_id}")
            return True
        else:
            logger.error(f"Failed to update item in Zoho: {item_id} - {response.text}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying update for item {item_id} in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Exhausted all retries for item {item_id}.")
                return False

# Function to fetch the purchase account ID from Zoho
def get_purchase_account_id():
    global purchase_account_id
    if purchase_account_id is None:
        url = f"{ZOHO_API_BASE_URL}/chartofaccounts?organization_id={ORGANIZATION_ID}"
        headers = get_zoho_headers()
        response = make_zoho_request("GET", url, headers)

        if response.status_code == 200:
            accounts = response.json().get("chartofaccounts", [])
            for account in accounts:
                if account['account_type'] == 'Cost of Goods Sold':  # Example filter
                    purchase_account_id = account['account_id']
                    break
            if not purchase_account_id:
                logger.error("Failed to find 'Cost of Goods Sold' account.")
        else:
            logger.error(f"Failed to retrieve purchase account ID from Zoho: {response.text}")
    return purchase_account_id

# Updated compare_items view
def compare_items(request):
    lightspeed_items = get_all_items()
    zoho_items = get_all_zoho_items()

    if not lightspeed_items or not zoho_items:
        return JsonResponse({"error": "Failed to fetch items from one or both APIs"}, status=400)

    lightspeed_dict = {item['manufacturerSku']: item for item in lightspeed_items}
    zoho_dict = {item['sku']: item for item in zoho_items}

    items_to_update = []
    items_to_create = []
    successful_updates = []
    failed_updates = []

    for sku, ls_item in lightspeed_dict.items():
        zoho_item = zoho_dict.get(sku)

        if zoho_item:
            fields_to_update = {}

            # Convert to appropriate types and compare purchase_rate and defaultCost
            ls_cost = ls_item.get('defaultCost')
            zoho_cost = zoho_item.get('purchase_rate')

            ls_cost = float(ls_cost) if ls_cost not in [None, ''] else 0.0
            zoho_cost = float(zoho_cost) if zoho_cost not in [None, ''] else 0.0

            if abs(ls_cost - zoho_cost) > 0.01:
                fields_to_update["purchase_rate"] = ls_cost
                fields_to_update["purchase_account_id"] = "2866866000000034003"  # Added Purchase Account ID

            # Compare description with name
            if normalize_value(ls_item.get('description')) != normalize_value(zoho_item.get('name')):
                fields_to_update["name"] = ls_item.get('description')

            # Compare price with rate
            ls_price = ls_item.get('price')
            zoho_rate = zoho_item.get('rate')

            ls_price = float(ls_price) if ls_price not in [None, ''] else 0.0
            zoho_rate = float(zoho_rate) if zoho_rate not in [None, ''] else 0.0

            if abs(ls_price - zoho_rate) > 0.01:
                fields_to_update["rate"] = ls_price

            if fields_to_update:
                items_to_update.append({
                    "item_id": zoho_item["item_id"],
                    "fields": fields_to_update
                })

        else:
            # Check if the item really does not exist in Zoho before attempting to create it
            existing_zoho_item = check_item_exists_in_zoho(sku, zoho_items)
            if not existing_zoho_item:
                # Ensure that the data being sent to Zoho is clean and valid
                name = ls_item.get("description")
                if name:
                    item_to_create = {
                        "name": name,
                        "rate": ls_item.get("price", 0),
                        "description": name,
                        "sku": sku,
                        "product_type": "goods",
                        "purchase_rate": ls_item.get("defaultCost", 0.0),
                        "purchase_account_id": "2866866000000034003",  # Added Purchase Account ID
                        "inventory_account_id": "2866866000000034001",  # Inventory Account ID
                        "item_type": "inventory",
                        "initial_stock": 1,  # Default initial stock, adjust as needed
                        "initial_stock_rate": ls_item.get("defaultCost", 0.0),  # Set to the Default Cost
                    }
                    items_to_create.append(item_to_create)
                else:
                    logger.warning(f"Skipping creation due to missing name for SKU {sku}.")
            else:
                logger.warning(f"Item with SKU {sku} already exists in Zoho, skipping creation.")

    logger.info(f"Total items to update: {len(items_to_update)}")
    logger.info(f"Total items to create: {len(items_to_create)}")

    headers = get_zoho_headers()

    # Process updates
    for idx, item in enumerate(items_to_update):
        success = update_item_in_zoho(item["item_id"], item["fields"], headers)
        if success:
            successful_updates.append(item)
        else:
            failed_updates.append(item)

        if (idx + 1) % 1000 == 0:
            logger.info(f"Processed {idx + 1} items. Successful updates: {len(successful_updates)}, Remaining: {len(items_to_update) - (idx + 1)}")

    logger.info(f"Total items to update: {len(items_to_update)}, Successful updates: {len(successful_updates)}, Failed updates: {len(failed_updates)}")

    # Process creations
    for idx, item in enumerate(items_to_create):
        response = make_zoho_request("POST", f"{ZOHO_API_BASE_URL}/items?organization_id={ORGANIZATION_ID}", headers, item)
        if response.status_code == 201:
            logger.info(f"Successfully created item in Zoho: {item['sku']}")
        elif response.status_code == 400 and "code" in response.json() and response.json()["code"] == 1001:
            logger.warning(f"Item with SKU {item['sku']} already exists in Zoho, skipping creation.")
        else:
            logger.error(f"Failed to create item in Zoho: {item['sku']} - {response.text}")

    return JsonResponse({
        "message": "Items processed.",
        "successful_updates": len(successful_updates),
        "failed_updates": len(failed_updates)
    })



# New view to handle user input for systemSku and update/create in Zoho
def update_or_create_specific_items(request):
    if request.method == "POST":
        form = SystemSkuForm(request.POST)
        if form.is_valid():
            skus = form.cleaned_data['systemSku'].split(',')
            skus = [sku.strip() for sku in skus]

            items_to_update = []
            items_to_create = []

            headers = get_zoho_headers()

            for sku in skus:
                # Get item details from Lightspeed using the SKU
                ls_item = get_lightspeed_item_details_by_sku(sku)
                if not ls_item:
                    logger.warning(f"No item found in Lightspeed for SKU {sku}, skipping.")
                    continue

                # Try to retrieve the item from Zoho using the SKU
                zoho_item = get_zoho_item_by_sku(sku)

                if zoho_item:
                    # Prepare the fields to update
                    fields_to_update = {}
                    ls_cost = float(ls_item.get('defaultCost', 0.0))
                    zoho_cost = float(zoho_item.get('purchase_rate', 0.0))

                    if ls_cost != zoho_cost:
                        fields_to_update["purchase_rate"] = ls_cost
                        fields_to_update["purchase_account_id"] = "2866866000000034003"  # Added Purchase Account ID
                        logger.info(f"Updating purchase_rate for SKU {sku}: {zoho_cost} -> {ls_cost}")

                    if normalize_value(ls_item.get('description')) != normalize_value(zoho_item.get('name')):
                        fields_to_update["name"] = ls_item['description']

                    ls_price = float(ls_item.get('price', 0.0))
                    zoho_rate = float(zoho_item.get('rate', 0.0))

                    if ls_price != zoho_rate:
                        fields_to_update["rate"] = ls_price

                    if fields_to_update:
                        items_to_update.append({
                            "item_id": zoho_item["item_id"],
                            "fields": fields_to_update
                        })
                else:
                    # Prepare item creation payload
                    item_to_create = {
                        "name": ls_item["description"],
                        "rate": ls_item["price"],
                        "description": ls_item["description"],
                        "sku": ls_item["manufacturerSku"],
                        "product_type": "goods",
                        "purchase_rate": ls_item.get("defaultCost", 0.0),  # Set the purchase rate to Default Cost
                        "purchase_account_id": "2866866000000034003",  # Added Purchase Account ID
                        "inventory_account_id": "2866866000000034001",  # Inventory Account ID
                        "item_type": "inventory",
                        "initial_stock": 1,  # Default initial stock, adjust as needed
                        "initial_stock_rate": ls_item.get("defaultCost", 0.0),  # Set to the Default Cost
                    }

                    items_to_create.append(item_to_create)

            # Create new items in Zoho
            for item in items_to_create:
                response = make_zoho_request("POST", f"{ZOHO_API_BASE_URL}/items?organization_id={ORGANIZATION_ID}", headers, item)
                if response.status_code == 201:
                    logger.info(f"Successfully created item in Zoho: {item['sku']}")
                elif response.status_code == 400 and "code" in response.json() and response.json()["code"] == 1001:
                    logger.warning(f"Item with SKU {item['sku']} already exists in Zoho, skipping creation.")
                else:
                    logger.error(f"Failed to create item in Zoho: {item['sku']} - {response.text}")

            # Update existing items in Zoho
            for item in items_to_update:
                update_item_in_zoho(item["item_id"], item["fields"], headers)

            return JsonResponse({"message": "Specified items processed. Check logs for details."})

    else:
        form = SystemSkuForm()

    return render(request, 'api/update_create_items.html', {'form': form})



def get_lightspeed_item_details_by_sku(sku):
    access_token = refresh_access_token()
    if not access_token:
        return None

    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.lightspeedapp.com/API/V3/Account/292471/Item.json?manufacturerSku={sku}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        if 'Item' in data:
            item = data['Item']
            if isinstance(item, list):
                item = item[0]
            return {
                "itemID": item.get("itemID"),
                "defaultCost": item.get("defaultCost"),
                "description": item.get("description"),
                "manufacturerSku": item.get("manufacturerSku"),
                "price": next((price.get("amount") for price in item.get("Prices", {}).get("ItemPrice", []) if price.get("useType") == "Default"), None)
            }
    logger.error(f"Failed to fetch item from Lightspeed for SKU {sku}: {response.text}")
    return None


def get_zoho_item_by_sku(sku):
    url = f"{ZOHO_API_BASE_URL}/items?organization_id={ORGANIZATION_ID}&sku={sku}"
    headers = get_zoho_headers()
    response = make_zoho_request("GET", url, headers)

    if response.status_code == 200:
        data = response.json()
        items = data.get("items", [])
        if items:
            return items[0]
        else:
            # Log as info, not error, because it's expected behavior
            logger.info(f"Item with SKU {sku} not found in Zoho. Proceeding to create the item.")
            return None
    else:
        logger.error(f"Failed to fetch item from Zoho for SKU {sku}: {response.text}")
        return None
