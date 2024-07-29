import pymongo
import pandas as pd
from datetime import datetime, timedelta, timezone


# Connexion à MongoDB
client = pymongo.MongoClient("mongodb+srv://intraday:intraday@dev.vqjrrab.mongodb.net/")  # Remplacez par votre URI MongoDB
db = client['DEV']  # Remplacez par le nom de votre base de données
collection = db['TEST']  # Remplacez par le nom de votre collection
dct_collection = db['DCT']  # Nouvelle collection pour traquer les opérations débit/crédit

time_threshold = datetime.now(timezone.utc) - timedelta(minutes=60)


# Récupérer uniquement les documents ajoutés dans les 30 dernières minutes
documents = list(collection.find({'STEP': {'$gte': time_threshold}}))

# Vérifiez si des documents sont récupérés
if not documents:
    print("Aucun document trouvé dans la collection pour les 30 dernières minutes.")
else:
    # Convertir les données en DataFrame pour un traitement plus facile
    df = pd.DataFrame(documents)

    # Assurer que 'STEP' est de type datetime et trier par 'ACCOUNT' et 'STEP'
    df['STEP'] = pd.to_datetime(df['STEP'])
    df = df.sort_values(by=['ACCOUNT', 'STEP'])


    # Initialiser des listes pour stocker les opérations de débit et de crédit
    debit_operations = []
    credit_operations = []

    # Parcourir chaque transaction et mettre à jour le solde disponible
    for account, group in df.groupby('ACCOUNT'):
        seen_operation_refs = set()  # Traquer les operation_ref déjà vus pour ce compte

        for index, row in group.iterrows():
            operation_ref = row['OPERATION_REF']
            
            sign = row['SIGN']
            amount = row['DISPONIBLE']
            old_amount = row['OLDDISPONIBLE']

            # Vérifier si le solde précédent était positif et le nouveau solde est ≤ 0
            if old_amount >= 0 and amount < 0:
                debit_operations.append({
                    'ACCOUNT': account,
                    'OPERATION_REF': operation_ref,
                    'STEP': row['STEP'],
                    'SIGN': sign,
                    'DISPONIBLE': amount,
                    'OLDDISPONIBLE' : row['OLDDISPONIBLE'],
                    'AMOUNT' : row['AMAOUNT']

                })
            # Vérifier si le solde précédent était négatif et le nouveau solde est ≥ 0
            if old_amount < 0 and amount >= 0:
                credit_operations.append({
                    'ACCOUNT': account,
                    'OPERATION_REF': operation_ref,
                    'STEP': row['STEP'],
                    'SIGN': sign,
                    'DISPONIBLE': amount,
                    'OLDDISPONIBLE' : row['OLDDISPONIBLE'],
                    'AMOUNT' : row['AMAOUNT']


                })



 

    # Insérer les opérations de débit et de crédit dans la collection DCT
    if debit_operations or credit_operations:
        dct_collection.insert_many(debit_operations + credit_operations)
        print(f"Opérations de débit/crédit insérées dans la collection DCT: {len(debit_operations) + len(credit_operations)}")

# Fermer la connexion
client.close()


