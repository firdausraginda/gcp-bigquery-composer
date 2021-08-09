import re


def print_row(row):
    """simply print the passing value"""

    print(row)


def remove_last_colon(row):
    """remove last semi colon on item name, per row"""

    cols = row.split(',')
    item = str(cols[4])

    if item.endswith(':'):
        cols[4] = item[:-1]

    return ','.join(cols)


def remove_special_characters(row):
    """remove special characters per row"""

    cols = row.split(',')
    string_result = ''

    for col in cols:
        clean_col = re.sub(r'[?%&]','',col)
        string_result = string_result + clean_col + ','
    
    string_result = string_result[:-1]
    
    return string_result


def to_json(csv_str):
    """convert dictionary format to json"""

    fields = csv_str.split(',')

    json_str = {
        "customer_id": fields[0],
        "date": fields[1],
        "timestamp": fields[2],
        "order_id": fields[3],
        "items": fields[4],
        "amount": fields[5],
        "mode": fields[6],
        "restaurant": fields[7],
        "status": fields[8],
        "ratings": fields[9],
        "feedback": fields[10],
        "new_col": fields[11]
    }

    return json_str