"""
ZADANIE WYSŁANE PONOWNIE
ZAPOMNIAŁEM DOPISAĆ O POPRAWCE W KODZIE TZN.
w data = 'side,currency,value\nIN,PLN,1\nIN,EUR,2\nOUT,ANY,3'
występuje wartośc "any", moim zdaniem to znaczy, że dotyczy to każej waluty, 
więc zaimplementowałem w kodzie to, że przy wartości side='out', kazda waluta
bedzie miala value='3'
"""

from typing import List, Dict, Any
import io


def task1(search: Dict[str, Any], data: str, cache: Dict = {}) -> str:
    cache_key = (frozenset(search.items()), data)
    if cache_key in cache:
        return cache[cache_key]

    data_stream = io.StringIO(data)
    
    try:
        header_line = next(data_stream)
        headlines = [h.strip() for h in header_line.split(',')]
    except StopIteration:
        return '-1' # Pusty plik

    search_keys_set = set(search.keys())
    if not search_keys_set.issubset(set(headlines)):
        raise Exception("Key mismatch")
    
    for line in data_stream:
        if line.strip():
            values = line.strip().split(',')
            if len(values) == len(headlines):
                element = dict(zip(headlines, values))
                
                is_match = True
                for key, value_to_find in search.items():
                    if str(element.get(key)) != str(value_to_find) and str(element.get(key)) != 'ANY':
                        is_match = False
                        break
                
                if is_match:
                    result = element.get('value', '-1')
                    cache[cache_key] = result
                    return result
            
    cache[cache_key] = '-1'
    return '-1'


"""
DANE DO SPRWADZENIA task1

data = 'side,currency,value\nIN,PLN,1\nIN,EUR,2\nOUT,ANY,3'
print(task1({'side': 'IN', 'currency': 'PLN'}, data))
print(task1({'side': 'IN', 'currency': 'EUR'}, data))
print(task1({'side': 'IN', 'currency': 'GBP'}, data))
print(task1({'side': 'OUT', 'currency': 'GBP'}, data))
print(task1({'side': 'OUT', 'currency': 'YSSSSSSl'}, data))
try:
    print(task1({'side': 'OUT', 'csdaurrency': 'YSSSSSSl'}, data))
except Exception as e:
    print(e)
"""


def task2(key_list: List[Dict[str, Any]], data: str) -> str:
    data_stream = io.StringIO(data)
    
    try:
        header_line = next(data_stream)
        headlines = [h.strip() for h in header_line.split(',')]
    except StopIteration:
        return "0.0"

    processed_data = []
    for line in data_stream:
        if line.strip():
            values = line.strip().split(',')
            if len(headlines) == len(values):
                processed_data.append(dict(zip(headlines, values)))
    
    value_list = []
    for search_criteria in key_list:
        for data_row in processed_data:
            is_match = True
            for key, value_to_find in search_criteria.items():
                if str(data_row.get(key)) != str(value_to_find) and str(data_row.get(key)) != 'ANY':
                    is_match = False
                    break
            
            if is_match:
                try:
                    value_list.append(int(data_row['value']))
                except (ValueError, KeyError):
                    pass
                break
    
    if not value_list:
        return "0.0"

    weights = [20 if v % 2 == 0 else 10 for v in value_list]
    
    sum_of = sum(v * w for v, w in zip(value_list, weights))
    sum_weight = sum(weights)
    average = sum_of / sum_weight if sum_weight > 0 else 0.0
    
    return f"{average:.1f}"


"""
DANE DO SPRAWDZENIA task2

print(task2(
    [
        {'side': 'IN', 'currency': 'PLN'},
        {'side': 'IN', 'currency': 'EUR'},
        {'side': 'OUT', 'currency': 'EUR'},
        {'side': 'IN', 'currency': 'EUR'},
        {'side': 'OUT', 'currency': 'PLN'},
    ], 
    'side,currency,value\nIN,PLN,1\nIN,EUR,2\nOUT,ANY,3'
))
"""


import lz4.frame

file = 'find_match_average.dat.lz4'
keys = [
    {'a': 862984, 'b': 29105, 'c': 605280, 'd': 678194, 'e': 302120},
    {'a': 20226, 'b': 781899, 'c': 186952, 'd': 506894, 'e': 325696}
]

with open(file, 'rb') as f:
    compressed_data = f.read()
        
decompress_data = lz4.frame.decompress(compressed_data).decode('utf-8')
        
result = task2(keys, decompress_data)
print(result)

  