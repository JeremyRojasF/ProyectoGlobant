# Proyecto de Migración de Datos

Este proyecto tiene como objetivo migrar datos históricos de archivos CSV a una base de datos PostgreSQL y crear un servicio de API REST para recibir nuevas transacciones.

## Características del Proyecto

- Migración de Datos Históricos: Permite mover los datos históricos de un archivo CSV a la base de datos PostgreSQL.

- Backup tablas: Permite realizar backups de las tablas creadas en PostgreSQL y guardarlo en formato AVRO.

- Restore tablas: Permite restaurar las tablas en PostgreSQL mediante archivos en formato AVRO.

- API REST para Nuevas Transacciones: Proporciona un servicio de API REST para recibir y validar nuevas transacciones, permitiendo inserciones individuales o en lotes de hasta 1000 filas.


## Tecnologías Utilizadas

- Python: Lenguaje de programación utilizado para implementar el proyecto.

- FastAPI: Framework de desarrollo web utilizado para crear el servicio de API REST.

- PostgreSQL: Sistema de gestión de bases de datos SQL utilizado para almacenar los datos migrados y las nuevas transacciones.

## Instrucciones de Instalación

1. Clona el repositorio del proyecto:

```shell
git clone https://github.com/JeremyRojasF/ProyectoGlobant.git