ContinentCode = {
    'CA': 'NA',
    'US': 'NA',
    'MX': 'NA',
    'BR': 'NA',
    'ES': 'EU',
    'UK': 'EU',
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
    'JP': 'FE'
}


def get_continent_code(region):
    return ContinentCode.get(region, {})
