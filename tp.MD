### Quelle est la différence entre l’utilisation d’un schema figé lors de la lecture des données et l’utilisation de l’option inferSchema de spark ? 
### Quels sont les avantages et les inconvénients de chaque approche ?

**Schema figé** Cela signifie que nous choisissons et initialisons nous-mêmes le schéma, les types et les noms de colonnes.

### Avantages:
 - Il lit les données rapidement, car il n’essaie pas de créer lui-même le schéma,
 - Même avec un nested json, nous pouvons bien contrôler la structure et ne pas obtenir d'erreurs ou de colonnes vides.

### Inconvénients:
 - Tout doit être fait à la main, cela nécessite un travail manuel.


**inferSchema** détermine automatiquement la structure des données lors de la lecture des données.

### Avantages:
- Puisqu’il crée automatiquement la structure, moins de travail est nécessaire.

### Inconvénients:
- Le code s'exécutera assez lentement lors de la création de la structure.
- Avec des schemes complexes, on peut avoir de nombreuses erreurs avec les données et les colonnes.

---


### Quel est le type de la colonne âge ? Est-il possible de le changer en un autre type ?

- Après avoir divisé l'âge et le sexe, nous avons attribué la colonne age au type de données int en utilisant la méthode part (part ("integra")).


### Quels sont les types des nouvelles colonnes ?

[```python
[('year', 'int'),
 ('month', 'int'),
 ('day', 'int'),
 ('sexe', 'string'),
 ('age', 'int'),
 ('application', 'string'),
 ('total_time_spent', 'bigint'),
 ('total_users', 'bigint')]

### Quel type de jointure à utiliser ? Quelle est la différence entre les types de jointures ?

tut mi ispolzuem left join. 

### Voici quelques types
    
 - inner join.
        la jointure la plus utilisée, car elle renvoie uniquement les lignes qui ont des valeurs correspondantes sur les deux tables, donc vous n'avez pas besoin d'acheter des lignes qui ont des valeurs correspondantes sur les deux tables.

 - left join

    Dans le tableau sélectionné à gauche, renvoie toutes les lignes qui correspondent, sinon, la bonne table sera simplement NULL.

 - Right join

     le même que celui de gauche, il ne renverra que toutes les lignes du tableau de droite et à gauche NUll s'il n'y a pas de correspondance.

 - full join

    Renverra toutes les lignes des tables de droite et de gauche. Remplira NULL dans les deux pays où il n’y a pas de correspondance.


###  Peut-on configurer timezone la session spark afin de le rendre plus facile ?

 - Lors de la création d'une session Spark, la configuration peut être effectuée à l'aide de Spark.sql.session.timeZone et choisissons ce dont nous avons besoin timezone.


def create_spark_session():
    return SparkSession.builder \
        .appName("spark") \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "Europe/Paris") \
        .getOrCreate()


### Quels types de transformation pouvons-nous utiliser ?
 - Lorsque nous avons créé l'index cologne, nous avons utilisé les méthodes Window et lag.
 Window et lag

Window regroupe les données pour effectuer nos opérations.

Lag: la fonction obtient la valeur de la ligne précédente dans le groupe.

def calculate_index(df):
    window_spec = Window.partitionBy("criterion", "variable").orderBy("timestamp")
    df = df.withColumn("index", col("value") - lag("value", 365).over(window_spec))
    window_spec_avg = Window.partitionBy("criterion", "variable").orderBy("timestamp").rowsBetween(-2, 2)
    df = df.withColumn("smoothed_index", avg("index").over(window_spec_avg))
    return df


### Que fait la fonction cache() ? et que fait la fonction persist() ?


 - Les fonctions cache() et persist() sont utilisées pour stocker le DataFrame ou le RDD en mémoire pour une utilisation ultérieure. Cela permet d'éviter des calculs répétés.

 - cache() : stocke un DataFrame ou RDD en mémoire. Équivalent à l’appel de persist (StorageLevel.MEMORY_ONLY).

 - persist() : Une fonction plus flexible qui vous permet de spécifier le niveau de stockage (par exemple, en mémoire, sur disque ou les deux).



