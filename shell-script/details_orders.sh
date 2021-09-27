#!/bin/bash
{try
python3 /home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/List_Order.py 
#python3 /home/bred_valenzuela/full_vtex/stop_compute_engine.py 

}{ catch
    echo "Finalizo con problemas"
}

echo "Finalizado"
