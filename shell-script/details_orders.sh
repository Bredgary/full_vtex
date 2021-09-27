#!/bin/bash

#function try()
##{
#    [[ $- = *e* ]]; SAVED_OPT_E=$?
#    set +e
#}

python3 /home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/List_Order.py 
#python3 /home/bred_valenzuela/full_vtex/stop_compute_engine.py 
echo "Finalizado"
