import pymongo
import numpy as np
import pandas as pd

# Connexion à MongoDB
client = pymongo.MongoClient("mongodb+srv://intraday:intraday@dev.vqjrrab.mongodb.net/")
db = client['RCT']
collection = db['MOUV']


# Requête pour extraire les transactions selon les critères
transactions = collection.find({
    "OLDDISPONIBLE": { "$gt": 0 },
    "DISPONIBLE": { "$lte" : 0},
    "CANAL" : "SYMON" 
})

# Convertir les transactions en DataFrame pandas
df = pd.DataFrame(list(transactions))

# Sauvegarder le DataFrame dans un fichier CSV
df.to_csv('transactions_filtrees.csv', index=False)


# Fermer la connexion
client.close()
