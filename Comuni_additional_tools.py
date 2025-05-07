import pandas as pd
import numpy as np
import os 
from tqdm import tqdm
from fuzzywuzzy import fuzz, process


#######################################
# Preparazione dati Comunali (472ms)
istat = pd.read_csv('data/codici_istat.csv', sep=';', encoding='Windows-1252')
base = pd.read_excel('data/tassonomia_comuni.xlsx').rename(columns={'Azione':'azione'})
piani_comunali = pd.read_parquet('data/piani_comunali.gzip')

tassonomia = pd.merge(base,
                      piani_comunali[['ID_tassonomia', 'azione', 'codice_macro', 
                                      'descrizione_codice_macro', 'numero_codice_campo', 
                                      'descrizione_codice_campo']].value_counts().reset_index().sort_values(by='ID_tassonomia'),
                     on = 'azione', 
                     how= 'right')
    
fp = 'outputComuni/'
os.makedirs(fp, exist_ok=True)


#######################################
# Azioni ordinate per utilizzo di UNA provincia (30ms)
user_input = input('Provincia da analizzare (inserire un nome di una Provincia coinvolta):') # e.g. 'Bologna'
choices = istat['Denominazione dell\'Unità territoriale sovracomunale \n(valida a fini statistici)'].unique()

    #Si possono scegliere più o meno opzioni, con il primo elemento [0] si seleziona la migliore scelta per somiglianza
provincia = process.extract(user_input.strip(), choices, limit=10)[0][0]  

lista_comuni = istat[istat['Denominazione dell\'Unità territoriale sovracomunale \n(valida a fini statistici)'] == provincia]['Codice Comune formato alfanumerico']

focus = piani_comunali[piani_comunali.codice_istat.isin(lista_comuni)]

rank = pd.merge(focus.ID_tassonomia.value_counts().reset_index().rename(columns={'count':'Frequenza'}).sort_values(by=['Frequenza','ID_tassonomia']), tassonomia, on ='ID_tassonomia').rename(columns={'ID_tassonomia':'Voce della tassonomia'})
    #Questo risultato può essere disposto dal valore minore al maggiore (o viceversa) della colonna "Frequenza", che è specifica al numero di azioni usate nella Provincia ricercata. 
rank.to_csv(f'{fp}{provincia}.csv')

print(f'Azioni ordinate per utilizzo della Provincia di {provincia} salvate in {fp}{provincia}.csv')
print(rank.head())
print()
#######################################
# Serie storica per una lista di voci della tassonomia selezionate dall'utente (109ms)
    # e.g. di input precompilato:
    # user_inputs = ['Agevolazioni tariffarie e contributi attivitа ricreative/culturali/aggregative/formative	',
    #                ' Adesione ai marchi familiari	',
    #                'Sentieristica Family',
    #                'Progetti di abbattimento delle barriere architettoniche, segnalazione grado di accessibilitа']
user_inputs = []

while True:
    entry = input("Azione da visualizzare (scrivi 'END' per terminare la lista): ")
    if entry.strip() == "END":
        break
    user_inputs.append(entry.strip())

print("Voci inserite:", user_inputs)
    
choices = tassonomia.azione

    # identificazione degli input tra le scelte possibili
if len(user_inputs) > 1:
    azione = [process.extract(a, choices)[0][0] for a in user_inputs ]
elif len(user_inputs): 
    azione = [process.extract(user_inputs[0], choices)[0][0]]

    # preparazione dati per la visualizzazione
def wrapper(text, words_per_line=3):    
    words = text.replace('/',' - ').split()
    lines_per_chunk = words_per_line%len(words) + 1  
    lines = [' '.join(words[i:i+words_per_line]) for i in range(0, len(words), words_per_line)]    
    chunks = ['<br>'.join(lines[i:i+lines_per_chunk]) for i in range(0, len(lines), lines_per_chunk)]    
    return chunks[0]
    
storia = pd.DataFrame(columns=['anno_compilazione'])
for a in azione:
    t = piani_comunali[piani_comunali.azione == a]
    #si deve "rompere" a causa della frequente lunghezza eccessiva
    try:
        a = wrapper(a)
    except:
        a
    t = t.anno_compilazione.value_counts().reset_index().rename(columns={'count':a})
    
    if storia.shape[0]==0:
        storia = t
    else:
        storia = pd.merge(storia, t, on='anno_compilazione', how='left')    
storia = storia.fillna(0).sort_values(by='anno_compilazione')
storia.to_csv(f'{fp}serie_storica.csv')
print(f'Serie storica per le azioni selezionate salvata in {fp}serie_storica.csv')
print(storia.head())
print()
    # visualizzazione 
import plotly.express as px

df_long = storia.melt(id_vars='anno_compilazione', value_vars=storia,
                 var_name='Voce della tassonomia', value_name='Value')
fig = px.line(
    df_long,
    x='anno_compilazione',
    y='Value',
    color='Voce della tassonomia',
    markers=True,
)
    
fig.update_layout(
    xaxis_title="Annualità",
    yaxis_title="Frequenza di utilizzo"
)
fig.write_html(f"{fp}serie_storica_azione.html")
print(f'Visualizzazione interattiva salvata in {fp}serie_storica_azione.html')
