# API Lista de ordenes

La API lista de ordenes no se debe confundir con el detalle de ordenes que será creado en otro directorio, la lista de ordenes se le debe dar paramentros para que se ejecute desde la fecha principal y hasta la fecha de finalización, esta API hay que ejecutar antes de ordenes, porque el detalle de ordenes consume de la cuadratura de lista de ordenes.

- Dentro del directorio shell-script se busca el directorio listOrder y se ejecutar de esta forma: **sh category.sh 2020 1 1 2020 1 2** no hay que agregar comas, puntos, nada, se debe ejecutar tal y como se muestra en el ejemplo, la fecha traducida sería 2022/01/01 - 2020/1/2 año mes y día.