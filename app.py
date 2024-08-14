from flask import Flask, render_template, request
from pymongo import MongoClient

app = Flask(__name__)

# Connexion à la base de données MongoDB
client = MongoClient('mongodb+srv://intraday:intraday@dev.vqjrrab.mongodb.net/')  # Remplacez par votre URI MongoDB si différent
db = client['DCT']
collection = db['DCT']

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/transactions', methods=['GET'])
def get_transactions():
    account_number = request.args.get('account_number')
    transactions = list(collection.find({'ACCOUNT': account_number}, {'_id': 0}))
    
    return render_template('transactions.html', account_number=account_number, transactions=transactions)

if __name__ == '__main__':
    app.run(debug=True)
