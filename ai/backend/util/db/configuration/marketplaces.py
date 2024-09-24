ContinentCode = {
    'CA': 'NA',
    'US': 'NA',
    'MX': 'NA',
    'BR': 'NA',
    'ES': 'EU',
    'UK': 'UK',
    'FR': 'EU',
    'BE': 'EU',
    'NL': 'EU',
    'DE': 'EU',
    'IT': 'EU',
    'SE': 'EU',
    'PL': 'EU',
    'EG': 'EU',
    'TR': 'EU',
    'SA': 'EU',
    'AE': 'EU',
    'IN': 'EU',
    'SG': 'FE',
    'AU': 'FE',
    'JP': 'FE',
    'EU': 'EU',
    'NA': 'NA',
    'FE': 'FE',
}

regions = {
    'NA': {
        'market': ['US'],
        'sales_channel': ['Amazon.com'],
        'sku': 'ussku',
        'country': 'US'
    },
    'UK': {
        'market': ['UK'],
        'sales_channel': ['Amazon.co.uk'],
        'sku': 'uksku',
        'country': 'UK'
    },
    'EU': {
        'market': ['DE','FR','IT','ES'],
        'sales_channel': ['Amazon.fr', 'Amazon.it', 'Amazon.de', 'Amazon.es'],
        'sku': 'desku',
        'country': 'DE'
    },
    'FE': {
        'market': ['JP'],
        'sales_channel': ['Amazon.co.jp'],
        'sku': 'jpsku',
        'country': 'JP'
    }
}


def get_continent_code(region):
    return ContinentCode.get(region, {})

def get_region_info(region):
    continent_code = get_continent_code(region)
    if continent_code:
        return regions.get(continent_code, {})
    else:
        return {}


