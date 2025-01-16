import re
from datetime import datetime
from pycon.exceptions import PyCongressException
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, quote



def check_result(result, key, soft: bool = False):
    """Check if a key is in a dictionary and return it if it is

    Args:
        result (dict): dictionary to check
        key (str): key to check for

    Returns:
        dict: dictionary containing the key
    """
    if not isinstance(result, dict):
        raise PyCongressException("Error parsing response: no data found")
    if key not in result:
        if soft is True:
            return False
        else:
            raise PyCongressException(f"Error parsing response: no {key} data found")
    return True


def remove_html_tags(text):
    if not text:
        return None
    clean = re.compile("<.*?>")
    return re.sub(clean, "", text)

def check_date_format(date: str):
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    try:
        datetime.strptime(date, date_format)
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DDTHH:MM:SSZ")
    return quote(date)

def process_report_id(report_string):
    # Adjusted regular expression to make the periods and "Part" section optional
    match = re.match(r"([A-Z]+)\.? Rept\.? (\d+)-(\d+)(, Part (\d+))?", report_string)
    if match:
        chamber = match.group(1)[0]  # Get only the first letter
        congress = match.group(2)
        number = match.group(3)
        part_number = match.group(5) if match.group(5) else "-1"  # Corrected to group(5) for part number
        # Format the extracted parts
        if part_number != "-1":
            formatted_id = f"{str(chamber).lower()}{number}-{part_number}-{congress}"
        else:
            formatted_id = f"{str(chamber).lower()}{number}-{congress}"
        return formatted_id
    elif report_string:
        return report_string
    else:
        return "-99"

def add_date_range_to_url(url, from_date=None, to_date=None):
    """
    Add fromDateTime and toDateTime parameters to a given URL.

    Args:
        url (str): The original URL
        from_date (str, optional): The start date in ISO 8601 format (e.g., '2023-01-01T00:00:00Z')
        to_date (str, optional): The end date in ISO 8601 format (e.g., '2023-12-31T23:59:59Z')

    Returns:
        str: The modified URL with date range parameters added
    """

    if not from_date and not to_date:
        return url
    # Parse the URL
    parsed_url = urlparse(url)

    # Get the existing query parameters
    query_params = parse_qs(parsed_url.query)

    # Add or update the date range parameters
    if from_date:
        query_params['fromDateTime'] = [from_date]
    if to_date:
        query_params['toDateTime'] = [to_date]

    # Encode the updated query parameters
    new_query = urlencode(query_params, doseq=True)

    # Reconstruct the URL with the new query parameters
    new_url = urlunparse(parsed_url._replace(query=new_query))

    return new_url

def replace_page_size(url, page_size):
    """
    Replace the page size parameter in a given URL.

    Args:
        url (str): The original URL
        page_size (int): The new page size

    Returns:
        str: The modified URL with the updated page size parameter
    """
    # Parse the URL
    parsed_url = urlparse(url)

    # Get the existing query parameters
    query_params = parse_qs(parsed_url.query)

    # Add or update the page size parameter
    query_params['limit'] = [str(page_size)]

    # Encode the updated query parameters
    new_query = urlencode(query_params, doseq=True)

    # Reconstruct the URL with the new query parameters
    new_url = urlunparse(parsed_url._replace(query=new_query))

    return new_url