from flask import Flask, render_template, request
import joblib
import numpy as np

# Chargement du modèle
model = joblib.load('fraud_detection_model.pkl')

# Dictionnaires pour mapper les valeurs de CANAL et TYPE
canal_mapping = {
    '1': [1, 0, 0],  # CANAL_1
    'SYHBK': [0, 1, 0],  # CANAL_SYHBK
    'SYMON': [0, 0, 1]   # CANAL_SYMON
}

type_mapping = {
    'C': [1, 0],  # TYPE_C
    'D': [0, 1]   # TYPE_D
}

# Initialisation de l'application Flask
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    prediction = None
    if request.method == 'POST':
        # Récupération des données du formulaire
        DISPONIBLE = float(request.form['DISPONIBLE'])
        OLDDISPONIBLE = float(request.form['OLDDISPONIBLE'])
        ACCOUNT = float(request.form['ACCOUNT'])
        AMAOUNT = float(request.form['AMAOUNT'])
        CANAL = request.form['CANAL'].upper()  # Convertir en majuscule pour une correspondance facile
        TYPE = request.form['TYPE'].upper()  # Convertir en majuscule pour une correspondance facile

        # Conversion des valeurs CANAL et TYPE
        canal_values = canal_mapping.get(CANAL, [0, 0, 0])  # Par défaut [0, 0, 0] si non trouvé
        type_values = type_mapping.get(TYPE, [0, 0])  # Par défaut [0, 0] si non trouvé

        # Création de la liste de caractéristiques
        features = np.array([[DISPONIBLE, OLDDISPONIBLE, ACCOUNT, AMAOUNT] + canal_values + type_values])

        # Prédiction
        prediction = model.predict(features)[0]

    # Rendu de la page avec le résultat de la prédiction
    return render_template('index2.html', prediction=prediction)

if __name__ == '__main__':
    app.run(debug=True)
