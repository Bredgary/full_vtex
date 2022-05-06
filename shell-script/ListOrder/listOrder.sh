#! /bin/bash

echo "Desde: $1 - $2 - $3"
echo "Hasta: $4 - $5 - $6 "

parameterA="$1" 
parameterB="$2" 
parameterC="$3" 

parameterD="$4" 
parameterE="$5" 
parameterF="$6" 

python3 /home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/HistoryListOrder.py $parameterA $parameterB $parameterC $parameterD $parameterE $parameterF