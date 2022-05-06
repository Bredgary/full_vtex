#! /bin/bash

echo "First arg: $1"
echo "Second arg: $2"

parameterA="$1" 
parameterB="$2" 

python3 /home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/HistoryListOrder.py 3 3