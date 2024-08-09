import numpy as np
import pandas as pd


df = pd.read_csv('transactions_filtrees.csv')
df = df[['DISPONIBLE', 'OLDDISPONIBLE', 'STEP', 'ACCOUNT', 'AMAOUNT', 'CANAL']]


# Calculer le nombre de lignes à modifier
rows_to_modify = int(0.8 * len(df))

# Sélectionner aléatoirement 80% des indices des lignes à modifier
indices_to_modify = np.random.choice(df.index, size=rows_to_modify, replace=False)

# Mettre à zéro les valeurs de la colonne 'OLDDISPONIBLE' pour les indices sélectionnés
df.loc[indices_to_modify, 'OLDDISPONIBLE'] = 0


df.to_csv('fraud.csv', index=False)
