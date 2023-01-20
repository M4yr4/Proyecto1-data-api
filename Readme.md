# <h1 align=center> **PROYECTO INDIVIDUAL No. 1**
#  <h1 align=center>**Data Engineering**  <h1>

Este proyecto consiste en situarnos el rol de Data Engineer, tomando 4 archivos CSV para hacer el proceso de ETL y luego acceder a los mismos mediante la elaboración y ejecución de una API.

Dividimos el proyecto en dos partes:
<br>
#### 1. Transformaciones
Importar las librerías necesarias

    import pandas as pd
    import numpy as np
    from fastapi import FastAPI
<br>
Cargar los archivos CSV

    df_Amazon = pd.read_csv(r'datasets/amazon_prime_titles-score.csv')
    df_Disney = pd.read_csv(r'datasets/disney_plus_titles-score.csv')
    df_Hulu = pd.read_csv(r'datasets/hulu_titles-score.csv')
    df_Netflix = pd.read_csv(r'datasets/netflix_titles-score.csv')
<br>
Crear la columna 'id' con la inicial de cada plataforma y concatenarlo con la columna 'show_id'

    df_Amazon['id'] = 'a'+df_Amazon['show_id']
    df_Disney['id'] = 'd'+df_Disney['show_id']
    df_Hulu['id'] = 'h'+df_Hulu['show_id']
    df_Netflix['id'] = 'n'+df_Netflix['show_id']
<br>
Unir todos los archivos en un solo Dataframe

    df_movies = pd.concat([df_Amazon, df_Disney,df_Hulu,df_Netflix])
<br>

En la columna 'Rating' reemplazar los valores nulos por el str "G"

    df_movies['rating'].fillna('G', inplace= True)

<br>

Cambiar el formato de las fechas a AAAA-mm-dd

    df_movies['date_added']= pd.to_datetime(df_movies['date_added'])
<br>

Pasar los campos de texto a minúsculas, sin excepciones

    columns= ["show_id", "type", "title", "director", "cast", "country", "listed_in", "description"] 
    for i in columns:
        df_movies[i]=df_movies[i].str.lower()
<br>
Cambiar a string el campo duration

    df_movies['duration']=df_movies['duration'].astype(str)
<br>

Dividir el campo 'Duration' en dos campos

    df_duration = df_movies.duration.str.split(expand=True)
    df_duration.columns = ['duration_int', 'duration_type']
    df_movies=pd.concat([df_movies, df_duration], axis=1)
<br>

Normalizar el campo duration_type a singular

    df_movies.duration_type.replace("Seasons", "season", inplace=True)
<br>

Convertir el campo a int

    df_movies['duration_int'] = df_movies['duration_int'].fillna(0).astype(float)
    df_movies['duration_int'] = df_movies['duration_int'].fillna(0).astype(int)

<br>
<br>

#### 2. Desarrollo API
En esta parte se crea la interfaz

    app = FastAPI()
    #http://127.0.0.1:8000

    @app.get("/")
    def index():
    return{"Proyecto de:":"MAYRA PAJARO MONTERO"}
<br>
Las consultas a realizar son las siguientes:
<br>

1. Cantidad de veces que aparece una keyword en el titulo de pelicula/serie, por plataforma

        @app.get("/get_word_count/{platform}/{keyword}")
        def get_word_count(platform : str, keyword: str):
                    
            result = df_movies[(df_movies['title'].str.contains(keyword)) &  (df_movies['id'].str.startswith(platform[0])==True)]
                cantidad = len(result)

                return "La palabra '"+keyword+"' aparece "+str(cantidad)+" veces en la plataforma "+platform

<br>
2. Cantidad de películas por plataforma con un puntaje mayor a XX en determinado año

    @app.get("/get_score_count/{platform}/{score}/{year}")
    def get_score_count(platform : str, score : int , year : int):

        result = df_movies[(df_movies['type']=="movie") & (df_movies['release_year']==year) & (df_movies['score']>score) &  (df_movies['id'].str.startswith(platform[0])==True)]
            cantidad = len(result)
        return "Existen "+ str(cantidad)+" peliculas del año "+str(year)+" en la plataforma "+platform

<br>
3. La segunda película con mayor score para una plataforma determinada, según el orden alfabético de los títulos

    @app.get("/get_second_score/{platform}")
    def get_second_score(platform):
        result = df_movies[(df_movies['type']=="movie") &  (df_movies['id'].str.startswith(platform[0])==True)]
        result = result.sort_values(by=['score', 'title'], ascending=[False,True])
        title = result.iloc[1,2]
        score = result.iloc[1,12]
        return "La segunda pelicula con mayor score en la plataforma "+platform+" es '"+title+"' con un puntaje de "+str(score)

<br>
4.  Película que más duró según año, plataforma y tipo de duración

    @app.get("/get_longest/{platform}/{duration_type}/{year}")
    def get_longest(platform: str,duration_type: str,year: int):

        result = df_movies[(df_movies['type']=="movie") &  (df_movies['release_year']==year) & (df_movies['duration_type']==duration_type) & (df_movies['id'].str.startswith(platform[0])==True)]

        duration_max = result['duration_int'].max()

        title = result[result['duration_int']==duration_max] ['title']
        title = title.to_list()[0]   
    
        return "La pelicula que mas duró en el año "+str(year)+" en la plataforma "+platform+" es '"+title+"' con "+str(duration_max)+" "+duration_type

<br>
5. Cantidad de series y películas por rating

    @app.get("/get_rating_count/{rating}")
    def get_rating_count(rating: str):
    
        result = df_movies[(df_movies['rating']==rating)]
        cant = len(result)
        return "Existen actualmente "+str(cant)+" series/peliculas con rating "+rating