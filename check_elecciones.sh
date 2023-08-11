#!/bin/bash

./check_table.py elecciones.variable -n eleccion valor
./check_table.py elecciones.isla -n eleccion id_isla total_diputados
./check_table.py elecciones.municipios -n eleccion id_municipio id_isla
./check_table.py elecciones.partidos -n eleccion id_partido
./check_table.py elecciones.candidatura -n eleccion id_isla id_partido orden
./check_table.py elecciones.coaliciones -n eleccion
./check_table.py elecciones.actas -n id_actas id_isla eleccion censados nulas blancos validos
./check_table.py elecciones.detalle_actas -n id_actas eleccion orden votos id_partido
./check_table.py elecciones.acumulado -n eleccion id_isla censados nulas blancos votos_partidos barrera
./check_table.py elecciones.dhont -n eleccion id_isla id_partido cociente cantidad escanio
