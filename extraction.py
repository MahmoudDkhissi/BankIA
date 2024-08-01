import pymongo
import pandas as pd

# Connexion à MongoDB
client = pymongo.MongoClient("mongodb+srv://intraday:intraday@dev.vqjrrab.mongodb.net/")
db = client['DEV']
collection = db['MOUV_COPIE']

# Récupérer toutes les transactions
documents = list(collection.find())
if not documents:
    print("Aucun document trouvé dans la collection.")
else:
    # Convertir les données en DataFrame pour un traitement plus facile
    df = pd.DataFrame(documents)
    df['STEP'] = pd.to_datetime(df['STEP'])
    df = df.sort_values(by=['ACCOUNT', 'STEP'])

    # Initialiser une liste pour stocker les données à exploiter dans le modèle ML
    ml_data = []

    # Parcourir chaque transaction et identifier les transactions débiteuses
    for account, group in df.groupby('ACCOUNT'):
        for index, row in group.iterrows():
    

            if (row['DISPONIBLE'] < 0 and row['OLDDISPONIBLE'] >= 0) :  # Transaction débiteuse
                # Récupérer les transactions précédentes (pas de restriction à 5)
                start_index = max(0, index-5)

                previous_transactions = group.iloc[start_index:index]

                
                # Calculer les caractéristiques agrégées
                sum_amount = previous_transactions['AMAOUNT'].sum()
                mean_amount = previous_transactions['AMAOUNT'].mean()
                min_amount = previous_transactions['AMAOUNT'].min()
                max_amount = previous_transactions['AMAOUNT'].max()
                num_debits = (previous_transactions['SIGN'] == 'D').sum()
                num_credits = (previous_transactions['SIGN'] == 'C').sum()

                # Ajouter les caractéristiques au dataset
                ml_data.append({
                    'account': account,
                    'step': row['STEP'],
                    'sum_amount': sum_amount,
                    'mean_amount': mean_amount,
                    'min_amount': min_amount,
                    'max_amount': max_amount,
                    'num_debits': num_debits,
                    'num_credits': num_credits,
                    'target': 'debit'  # Label pour ML
                })

    # Convertir les données en DataFrame pour les exploiter dans un modèle ML
    ml_df = pd.DataFrame(ml_data)
    
    # Afficher un aperçu des données
    print(ml_df.head())

    ml_df.to_csv('ml_data.csv', index=False)


# Fermer la connexion
client.close()
