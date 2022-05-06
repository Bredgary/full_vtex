## API Dependiente

Para poder ejecutar la API de productos, hay que ejecutar algunas APIS, la shell ya está configurada para ejecutar estos 3 procesos sin tener que ir al directorio:

```mermaid
Category
La ramificación de categoría se debe cargar debido a que la API que proporciona los ID's de productos consumen 
de está tabla para almacenar en una tabla temporal para posteriormente obtener el detalle de cada producto 
```

```mermaid
Get_Product_and_SKU_IDs
Esta es la API que consume los ID's de categoría para devolver los ID's de productos
```

```mermaid
Get_Product_by_ID
Finalmente, en esta API nos retorna el detalle de cada producto
```

