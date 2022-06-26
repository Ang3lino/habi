**Examen Data Engineer**


*Ing. Angel Manriquez*


## Entregables

Cuando finalices el ejercicio crea una rama con tu nombre y el prefijo `_solution` ejemplo: `homero_simpson_solution`, envía un Pull Request y notifica de la entrega del ejercicio. 

🐍🐍🐍


## Para ejecutar
Cargar el codigo MySQL en **actors/actors/sql** pues contiene la creacion de la base de datos y procedimientos usados por el controlador.

Opcionalmente se puede crear un entorno, para las pruebas se uso conda.

Colocar el nuevo excel con nombre "Matriz_de_adyacencia_data_team.xlsx" en actors/res.

Instalar los requerimientos requirements.txt.

Crear la base de datos postgres con los siguientes parametros (se pueden cambiar acorde a los que se tiene en cada dispositivo en dagster_home/dagster.yaml):
```
         username: 'postgres'
         password: 'dagster'
         hostname: 'localhost'
         db_name: 'dagster'
```

Los logs estan en dagster_home.

Situarse en actors/ y ejecutar el demonio para el schedule.
```
nohup dagster-daemon run >/dev/null 2>&1 & disown
```

Se pueden correr las pruebas con pytest.

Ejecutar dagit, el archivo yaml para el proyecto es *conf.yaml*, configurarlo acorde a las necesidades. Para el recurso mysql son las credenciales para crear las bases de datos con base en el Excel.

```
resources:
  excel:
    config:
      excel_name: "Matriz_de_adyacencia_data_team.xlsx"
  mysql:
    config: 
       username: 'root'
       password: 'root'
       hostname: 'localhost'
       db_name: 'actors'
```
