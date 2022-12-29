from airflow.models import DAG
from airflow.operators.python import PythonOperator


#from nltk.tokenize import WordPunctTokenizer
import pandas as pd
import numpy as np



import json


DATA_PATH = '/home/mehdi_hr/AirflowHome/data/projet_final'
TMP = DATA_PATH + '/tmp'
OUTPUT = DATA_PATH + '/output'

# scrap du contenu de chaque article 
def getdata(url):
    import requests
    r = requests.get(url)
    return r.text

def _scrap_data():
    
    import requests
    from bs4 import BeautifulSoup
    
    """## La vie eco"""

    l = []
    c = []
    d = []
    a = []

    page = requests.get('https://www.lavieeco.com/economie/')
    bsoup = BeautifulSoup(page.content,'html.parser')
    info = bsoup.find_all(class_="title")
    for inf in info:
        url = inf.find('a').get('href')
        l.append(url)

        
    for lien in l:# les liens precedents deja stockés dans l
            
        htmldata = getdata(lien)
        soup = BeautifulSoup(htmldata, 'html.parser')
        
        time = soup.find(class_="post-published updated") #rechrche des dates
        d.append(time.find('b').text) # stockage des dates des articles

        # scrap des autheurs

        span = soup.findAll('span',attrs={"class":"post-author-name"}) # rechercher la classe qui contient le nom d'autheur
        for x in span:
            a.append(x.find('b').text) # enregistrement des noms des auteur dans la liste a


        # scrap du contenu des articles " contenu est composé de plusieur paragraph <p>"

        contenu=""
        div = soup.findAll('div',attrs={"class":"entry-content clearfix single-post-content"}) # rechercher la classe qui contient les paragraphes
        for x in div:
            ab=x.find_all('p') # resortir l'ensemble des paragraph dans la classe
            for elem in ab:
                contenu+=elem.text # concatenation des paragraphes
            c.append(contenu)  # enregistrement des paragraphe dans c

    df1 = pd.DataFrame()
    df1['lien'] = l
    df1['contenu'] = c
    df1['journal'] = 'La Vie Eco'
    df1['auteur'] = a
    df1['date'] = d
    df1

    """## Challenge"""

    l = []
    c = []
    d = []
    a = []

    page = requests.get('https://www.challenge.ma/category/economie/')
    bsoup = BeautifulSoup(page.content,'html.parser')

    # scrap des liens qui menent vers les articles economiques, les stocker dans une liste l
    info = bsoup.findAll('h3',attrs={"class":"vw-post-box-title"})

    for inf in info:
        url = inf.find('a').get('href')
        l.append(url)

    for lien in l:# les liens precedents deja stockés dans l
            
        htmldata = getdata(lien)
        soup = BeautifulSoup(htmldata, 'html.parser')
        
        # scrap des dates
    
        time = soup.find("time").text #rechrche des dates
        d.append(time) # stockage des dates des articles dans d
        
        #scrap des autheurs

        aut = soup.find('a',attrs={"class":"author-name"}).text
        a.append(aut)
        
        # scrap du contenu 
        contenu=""
        div = soup.findAll('div',attrs={"class":"vw-post-content clearfix"}) # rechercher la classe qui contient les paragraphes
        for x in div:
            ab=x.find_all('p') # resortir l'ensemble des paragraph dans la classe
            for elem in ab:
                contenu+=elem.text # concatenation des paragraphes
            c.append(contenu)  # enregistrement des paragraphe dans c

    df2 = pd.DataFrame()
    df2['lien'] = l
    df2['contenu'] = c
    df2['journal'] = 'Challenge'
    df2['auteur'] = a
    df2['date'] = d
    df2

    """## Le Matin

    """

    l = []
    c = []
    d = []
    a = []

    pages = np.arange(1,6,1)# Que les 6 premiers pages
    for page in pages:
    # le lien du volet economique du site

        page = requests.get('https://lematin.ma/journal/economie/'+str(page))
        bsoup = BeautifulSoup(page.content,'html.parser')
        info = bsoup.find_all(class_="card h-100") # class qui contient les articles

        for inf in info:
            url = inf.find('a').get('href') # recuperation du lien de chaque article
            l.append(url)

    for lien in l:# parcourir les liens precedents qui sont deja stockés dans l
            
        htmldata = getdata(lien)
        soup = BeautifulSoup(htmldata, 'html.parser')

        names = soup.findAll('p',attrs={"class":"author"})

        for inf in names:
            aut = inf.find('meta').get('content') 
            a.append(aut) # enregistrement des autheurs
        
        
        
        time = soup.find("time").text #recherche des dates
        d.append(time) # stockage des dates des articles dans la liste d
    

        div = soup.findAll('div',attrs={"class":"card-body p-2"}) # rechercher la classe qui contient les paragraphes
        for x in div:
            c.append(x.find('p').text)# enregistrement des contenus des articles dans c

    df3 = pd.DataFrame()
    df3['lien'] = l
    df3['contenu'] = c
    df3['journal'] = 'Le Matin'
    df3['auteur'] = a
    df3['date'] = [i[0:i.find('à')].strip() for i in d]
    


    df = pd.DataFrame()
    df = df.append(df1.append(df2.append(df3)))
    from encodings.utf_8 import encode
    from textwrap import indent
    with open(TMP + '/scrapped.json', 'w+', encoding='utf-8') as e:
        json.dump(df.to_json(orient="records", indent=2, force_ascii=False),e)





def clean_text(text):
    import nltk
    nltk.download('popular')
    import string
    from nltk.stem import WordNetLemmatizer  
    from nltk.corpus import stopwords
    #Charger les stopwords francais
    filtre_stopfr =  lambda text: [token for token in text if token.lower() not in french_stopwords]
    french_stopwords = set(stopwords.words('french'))
    lemmatizer = WordNetLemmatizer()
    #Enlever la ponctuation
    text = text.translate(str.maketrans('','',string.punctuation))
    #Enlever les caracteres numeriques
    text = text.translate(str.maketrans('','','123456789'))
    from nltk import word_tokenize
    tokens= filtre_stopfr( word_tokenize(text, language="french") )    
    #Lemmatisation
    tokens = [lemmatizer.lemmatize(token) for token in tokens]
    return ' '.join(tokens)

def _clean_data():
    import json
    articles = ''
    with open(TMP + '/scrapped.json', 'r+') as f:
        articles = json.loads(f.read())
    print(articles)
    df = pd.read_json(articles)
    print(df)
    import dateparser
    from datetime import datetime
    df['jour'] = [dateparser.parse(i).strftime('%d-%m-%Y') for i in df['date']]
    df['mois'] = [dateparser.parse(i).strftime('%m-%Y') for i in df['date']]
    df['annee'] = [dateparser.parse(i).strftime('%Y') for i in df['date']]
    # del df['date']
    # del df['lien']
    df['id'] = df.index + 1
    df = df[['id', 'contenu', 'journal', 'auteur','jour', 'mois','annee']]
    df['contenu'] = [clean_text(c) for c in df['contenu']]

    from encodings.utf_8 import encode
    from textwrap import indent
    with open(TMP + '/cleaned.json', 'w+', encoding='utf-8') as e:
        json.dump(df.to_json(orient="records", indent=2, force_ascii=False),e)

#Calcule la frequence 
def get_frequence(terme, document):
  return document.split(' ').count(terme)

# Fonction qui calcule TF
def TF(terme, document):
  return document.split(' ').count(terme)/len(document.split(' '))

# Fonction qui calcule IDF
def IDF(terme, corpus):
  import math
  N = len(corpus)
  i = 0
  for document in corpus:
    if document.count(terme) > 0:
      i = i + 1
  return math.log10(N/i)

# Fonction qui calcule TF-IDF
def TF_IDF(terme, document, corpus):
  return TF(terme, document) * IDF(terme, corpus)

def _generate_tables():


    df = pd.DataFrame()
    with open(TMP + '/cleaned.json') as f:
        df = pd.read_json(json.loads(f.read()))

    # Termes uniques
    termes = []
    for article in df['contenu']:
        termes.extend(article.split(' '))
    termes = list(set(termes))

    tf_idf = pd.DataFrame()
    tf_idf['article_id'] = df['id'] 
    frequence = pd.DataFrame()
    frequence['article_id'] = df['id'] 
    tf = pd.DataFrame()
    tf['article_id'] = df['id'] 
    idf = pd.DataFrame()
    idf['article_id'] = df['id'] 

    for terme in termes:
        tf_idf[terme] = [TF_IDF(terme, document, df['contenu']) for document in df['contenu']]
        tf[terme] = [TF(terme, document) for document in df['contenu']]
        idf[terme] = [IDF(terme,df['contenu']) for document in df['contenu']]
        frequence[terme] = [get_frequence(terme, document) for document in df['contenu']]

    frequence = frequence.set_index('article_id')
    tf = tf.set_index('article_id')
    idf = idf.set_index('article_id')
    tf_idf = tf_idf.set_index('article_id')

    #Termes devient un dataframe et represente une dimension
    termes = pd.DataFrame()
    termes['valeur'] = frequence.columns
    termes['terme_id'] = termes.index + 1
    termes = termes[['terme_id','valeur']]

    #Sous Dimension Date
    dates = pd.DataFrame()
    dates = df.drop_duplicates(subset=['jour','mois','annee'])
    dates['date_id'] = dates.index + 1
    dates = dates[['date_id','jour','mois','annee']]

    #Sous Dimension Auteurs
    auteurs = pd.DataFrame()
    auteurs = df.drop_duplicates(subset=['auteur','journal'])
    auteurs['auteur_id'] = auteurs.index + 1
    auteurs = auteurs[['auteur_id','auteur','journal']]
    
    #Dimension Article
    articles = df
    articles['article_id'] = df['id']
    del articles['id']
    articles = articles[['article_id','contenu','journal','auteur','jour','mois','annee']]

    articles = pd.merge(pd.merge(articles, dates),auteurs)
    articles = articles[['article_id','contenu','auteur_id','date_id']]

    #Fait Mesures
    mesures = pd.merge(termes,articles,how='cross',sort=False)

    mesures['frequence'] = [frequence.loc[i,str(j)] for i,j in zip(mesures['article_id'],mesures['valeur'])]
    mesures['tf'] = [tf.loc[i,j] for i,j in zip(mesures['article_id'],mesures['valeur'])]
    mesures['idf'] = [idf.loc[i,j] for i,j in zip(mesures['article_id'],mesures['valeur'])]
    mesures['tf_idf'] = [tf_idf.loc[i,j] for i,j in zip(mesures['article_id'],mesures['valeur'])]

    mesures = mesures[['terme_id','article_id','frequence','tf','idf','tf_idf']]
    mesures = mesures.sort_values(by=['tf_idf'], ascending=False)

    #Generation des tables
    with open(OUTPUT + '/termes.json', 'w+', encoding='utf-8') as e:
        json.dump(termes.to_json(orient="records", indent=2, force_ascii=False),e)

    with open(OUTPUT + '/dates.json', 'w+', encoding='utf-8') as e:
        json.dump(dates.to_json(orient="records", indent=2, force_ascii=False),e)

    with open(OUTPUT + '/auteurs.json', 'w+', encoding='utf-8') as e:
        json.dump(auteurs.to_json(orient="records", indent=2, force_ascii=False),e)

    with open(OUTPUT + '/articles.json', 'w+', encoding='utf-8') as e:
        json.dump(articles.to_json(orient="records", indent=2, force_ascii=False),e)

    with open(OUTPUT + '/mesures.json', 'w+', encoding='utf-8') as e:
        json.dump(mesures.to_json(orient="records", indent=2, force_ascii=False),e)    



def _delete_tmp_files():
    import shutil
    shutil.rmtree(TMP)    
    
from datetime import datetime
with DAG("my_dag",start_date=datetime(2022, 6, 11), schedule_interval="@once", catchup=True ) as dag:
    
    scrap_data = PythonOperator(
        task_id = "scrap_data",
        python_callable = _scrap_data
    )

    clean_data = PythonOperator(
        task_id = "clean_data",
        python_callable = _clean_data
    )

    generate_tables = PythonOperator(
        task_id = "generate_tables",
        python_callable = _generate_tables
    )

    delete_tmp_files = PythonOperator(
        task_id = "delete_tmp_files",
        python_callable = _delete_tmp_files
    )

    scrap_data >> clean_data >> generate_tables >> delete_tmp_files

