import pymongo
import pandas as pd
from datetime import datetime

# Connexion à MongoDB
client = pymongo.MongoClient("mongodb+srv://intraday:intraday@dev.vqjrrab.mongodb.net/")  # Remplacez par votre URI MongoDB
db = client['DEV']  # Remplacez par le nom de votre base de données
collection = db['MOUV_COPIE']  # Remplacez par le nom de votre collection
dct_collection = db['DCT']  # Nouvelle collection pour traquer les opérations débit/crédit

# Récupérer uniquement les documents non suivis (tracked: false ou tracked: null)
documents = list(collection.find({'$or': [{'tracked': False}, {'tracked': {'$exists': False}}, {'tracked': None}]}))

# Vérifiez si des documents sont récupérés
if not documents:
    print("Aucun document non suivi trouvé dans la collection.")
else:
    # Convertir les données en DataFrame pour un traitement plus facile
    df = pd.DataFrame(documents)

    # Assurer que 'STEP' est de type datetime et trier par 'ACCOUNT' et 'STEP'
    df['STEP'] = pd.to_datetime(df['STEP'])
    df = df.sort_values(by=['ACCOUNT', 'STEP'])

    # Initialiser des dictionnaires pour stocker les dernières transactions de débit et de crédit par compte
    last_transitional_operations = {}

    # Parcourir chaque transaction et mettre à jour le solde disponible
    for account, group in df.groupby('ACCOUNT'):
        for index, row in group.iterrows():
            operation_ref = row['OPERATION_REF']
            sign = row['SIGN']
            amount = row['DISPONIBLE']
            old_amount = row['OLDDISPONIBLE']

            # Vérifier si le solde précédent était positif et le nouveau solde est ≤ 0 (transition à D)
            if old_amount >= 0 and amount < 0:
                last_transitional_operations[account] = {
                    'ACCOUNT': account,
                    'OPERATION_REF': operation_ref,
                    'STEP': row['STEP'],
                    'SIGN': 'D',
                    'DISPONIBLE': amount,
                    'OLDDISPONIBLE': old_amount,
                    'AMOUNT': row['AMAOUNT']
                }
            # Vérifier si le solde précédent était négatif et le nouveau solde est ≥ 0 (transition à C)
            elif old_amount < 0 and amount >= 0:
                last_transitional_operations[account] = {
                    'ACCOUNT': account,
                    'OPERATION_REF': operation_ref,
                    'STEP': row['STEP'],
                    'SIGN': 'C',
                    'DISPONIBLE': amount,
                    'OLDDISPONIBLE': old_amount,
                    'AMOUNT': row['AMAOUNT']
                }

    # Insérer ou mettre à jour les dernières opérations transitoires dans la collection DCT
    for operation in last_transitional_operations.values():
        dct_collection.replace_one(
            {'ACCOUNT': operation['ACCOUNT']},
            operation,
            upsert=True
        )

    print(f"Opérations transitoires insérées ou mises à jour dans la collection DCT: {len(last_transitional_operations)}")

    # Mettre à jour l'attribut tracked à true pour les documents traités
    collection.update_many({'_id': {'$in': df['_id'].tolist()}}, {'$set': {'tracked': True}})

# Fermer la connexion
client.close()