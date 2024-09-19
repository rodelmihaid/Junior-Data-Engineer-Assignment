import pandas as pd
import re
from fuzzywuzzy import fuzz

facebook_data = pd.read_csv(r'C:\Users\User\Desktop\jrDataEngineer\facebook_dataset.csv', on_bad_lines='skip')
google_data = pd.read_csv(r'C:\Users\User\Desktop\jrDataEngineer\google_dataset.csv', on_bad_lines='skip', low_memory=False)

facebook_data = facebook_data.rename(columns={'categories': 'category'})

df_combined = pd.merge(facebook_data, google_data, on='domain', how='inner', suffixes=('_fb', '_google'))

website_data = pd.read_csv(r'C:\Users\User\Desktop\jrDataEngineer\website_dataset.csv', delimiter=';', on_bad_lines='skip')

# Convertim coloanele la litere mici
df_combined = df_combined.applymap(lambda s: s.lower() if type(s) == str else s)
website_data = website_data.applymap(lambda s: s.lower() if type(s) == str else s)

df_final_combined = pd.merge(df_combined, website_data, left_on='domain', right_on='root_domain', how='inner')

# Funcție pentru a aplica fuzzy matching intre două valori
def fuzzy_match_score(val1, val2):
    if pd.isna(val1) or pd.isna(val2):
        return 0  # Dacă una dintre valori este NaN, scorul este 0
    return fuzz.token_set_ratio(val1, val2)  # Folosește fuzzy matching cu fuzzywuzzy

# Funcție pentru a verifica dacă minim 3 dintre coloane (name, category, country, city) au peste 80% asemănare
def is_relevant_match(row):
    # Compară 'name' din facebook/google cu 'legal_name' din website
    score_name_fb = fuzzy_match_score(row['name_fb'], row['legal_name']) if 'name_fb' in row and 'legal_name' in row else 0
    score_name_google = fuzzy_match_score(row['name_google'], row['legal_name']) if 'name_google' in row and 'legal_name' in row else 0

    # Compară 'category' din facebook/google cu 's_category' din website
    score_category_fb = fuzzy_match_score(row['category_fb'], row['s_category']) if 'category_fb' in row and 's_category' in row else 0
    score_category_google = fuzzy_match_score(row['category_google'], row['s_category']) if 'category_google' in row and 's_category' in row else 0

    # Compară 'country_name' cu 'main_country' și 'city' cu 'main_city'
    score_country = fuzzy_match_score(row['country_name'], row['main_country']) if 'country_name' in row and 'main_country' in row else 0
    score_city = fuzzy_match_score(row['city'], row['main_city']) if 'city' in row and 'main_city' in row else 0

    # Dacă unul dintre scoruri este 100%, păstrează rândul direct
    if score_name_fb == 100 or score_name_google == 100 or score_category_fb == 100 or score_category_google == 100 :
        return True

    # Verificăm dacă minim trei dintre aceste scoruri sunt peste 80%
    match_count = sum([score_name_fb >= 80, score_name_google >= 80, score_category_fb >= 80, score_category_google >= 80,
                       score_country >= 80, score_city >= 80])
    return match_count >= 3

# Aplică funcția pentru a păstra doar rândurile relevante
df_relevant = df_final_combined[df_final_combined.apply(is_relevant_match, axis=1)]

# Funcție pentru a selecta valoarea non-NaN dintre două coloane duplicate
def choose_relevant_value(col_fb, col_google):
    if pd.isna(col_fb):
        return col_google
    return col_fb

for column in df_relevant.columns:
    if re.search('_fb$', column):  # Identifică coloanele care se termină cu '_fb'
        col_base = column[:-3]  # Numele de bază al coloanei fără sufix
        if col_base + '_google' in df_relevant.columns:  # Verifică dacă există și coloana cu sufix '_google'
            # Crează o coloană combinată fără sufix, păstrând valoarea relevantă
            df_relevant[col_base] = df_relevant.apply(lambda row: choose_relevant_value(row[column], row[col_base + '_google']), axis=1)
            # Elimină coloanele duplicate
            df_relevant = df_relevant.drop(columns=[column, col_base + '_google'])

df_relevant_no_duplicates = df_relevant.drop_duplicates(subset=['domain'])

df_relevant_no_duplicates.to_csv('merged_dataset.csv', index=False)

print(df_relevant_no_duplicates)
