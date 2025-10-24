# %%
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import sqlite3 



# %%
#get data from database

db_path = 'data/immobilier.db'
conn = sqlite3.connect(db_path)

df = pd.read_sql("SELECT * FROM realestate", conn)

print("/n aperçu des données :")
print(df.head())
print(df.shape)



   
# %%
#**Analyse des données**
df.isnull().sum()
df.info()
# %%
for col in df.columns:
    print(col)
    print(df[col].unique()[:5])
    print(df[col].nunique())
    print("-----")
# %%
print(df['type'].unique())
# %%
df['type'] = df['type'].replace({
    'appartement': 'Appartements',
    'villa': 'Villas',
})
# %%
#retirer la colonne id du dataset
df = df.drop(columns=['id', 'source'])
print(df.columns)
# %%
#visualisation de la distribution des prix
plt.figure(figsize=(10,6))
sns.histplot(df['price'], bins=20, kde=True)
# %%
df = df.drop_duplicates()

# %%
df.duplicated().sum()
# %%
#visualisation de la distribution pour chaque type de bien
print(df.type.value_counts())
df['type'].value_counts().plot(kind='bar')
# %%
df.columns = df.columns.str.lower().str.replace(' ', '_') 

# %% 
#drop ligne qui ont des valeurs Unknown et immobilier dans le colonne type
df = df[~df['type'].isin(['Unknown', 'Immobilier'])]
# %%
df.head()
# %%
strings = list(df.dtypes[df.dtypes == 'object'].index)
#uniformise tous les donnees dans les colonnes objects
for col in strings:
    df[col] = df[col].str.lower().str.replace(' ', '_')
# %%
df.head(50)
 # %%
sns.histplot(df['nombre_sdb'], bins=50)
# %%
#visualisation de la distribution pour chaque category
print(df.category.value_counts())
df['category'].value_counts().plot(kind='bar')
# %%
#comte le combre de valeurs dans superficie inferieux a 50
#print((df['superficie'] < 100).sum())
# %%
#supprimer les chambres
#df = df[~df['type'].isin(['chambres', 'chambre'])]

# %%
median_superficie = df['superficie'].median()
df.loc[(df['superficie'] < 20) & (df['type'] == 'appartements'), 'superficie'] = median_superficie

# %%
df.loc[(df['superficie'] < 20) & (df['type'] == 'appartements_meublés'), 'superficie'] = median_superficie
# %%
df.head(50)

# %%
df.superficie.unique()
# %%
def boxplot(df):
    plt.figure(figsize = (16,6))
    sns.boxplot(data = df)
    plt.grid()
# %%
boxplot(df[['price', 'superficie', 'nombre_chambres', 'nombre_sdb']])
# %%
#remplacer les outliers
features = ['price', 'superficie', 'nombre_chambres', 'nombre_sdb']
def impute_outliers(df, feature):
    q1 = np.percentile(df[feature], 25)
    q3 = np.percentile(df[feature], 75) 
    iqr = q3 - q1
    lower_bound = q1 - 1.5*iqr
    upper_bound = q3 + 1.5*iqr
    
    df.loc[df[feature] < lower_bound, feature] = lower_bound
    df.loc[df[feature] > upper_bound, feature] = upper_bound
# %%
for feature in features:
    impute_outliers(df, feature)
# %%
boxplot(df[features])
# %%
print(df.superficie.unique())
##encodage des varibales catégorielles
# %%
def histogram(df):
    return df.hist(bins=30, figsize=(18,10))
# %%
#histogram(df)
#sns.heatmap(df.corr(), annot=True, cmap='coolwarm')
# %%
from sklearn.preprocessing import OneHotEncoder

categorical_cols = ['type', 'category', 'area', 'city']
numerical_cols = ['superficie', 'nombre_chambres', 'nombre_sdb']
X_num = df[numerical_cols]
X_cat = df[categorical_cols]


#One hot encoding
encoder = OneHotEncoder(sparse_output=False, drop='first', handle_unknown='ignore')
X_cat_encoded = encoder.fit_transform(X_cat)
feature_names = encoder.get_feature_names_out(categorical_cols)

# %%
X_cat_df = pd.DataFrame(X_cat_encoded, columns=feature_names, index=df.index)
X_final = pd.concat([X_num, X_cat_df], axis=1)
X_final = pd.concat([X_num, X_cat_df], axis=1)
y = df['price']
# %%
print(X_final.head())
print(y.head())
# %%
#concate X_final et y
data = pd.concat([X_final, y], axis=1)
print(data.head())
# %%
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
mms = MinMaxScaler()
df_mms = pd.DataFrame(mms.fit_transform(data), columns=data.columns)
# %%
print(df_mms.price.head())
# %%
#histogram(df_mms)
# %%
#sns.displot(data= df_mms)
# %%
#separer les features et la target
X = df_mms.drop(columns=['price'])
y = df_mms['price']
# %%
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import time
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.tree import DecisionTreeRegressor

# %%
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(f"Train set size: {X_train.shape}, Test set size: {X_test.shape}")
# %%
models = {
    'Linear Regression': LinearRegression(),
    'Ridge Regression': Ridge(alpha=10.0),
    'Lasso Regression': Lasso(alpha=0.1, max_iter=10000),
    'Decision Tree': DecisionTreeRegressor(max_depth=15, min_samples_split=10, random_state=42),
    'Random Forest': RandomForestRegressor(n_estimators=100, max_depth=20, min_samples_split=5, random_state=42),
    'Gradient Boosting': GradientBoostingRegressor(n_estimators=100, max_depth=5, learning_rate=0.1, random_state=42)   
}


# %%
print(f"{len(models)} modèles initialisés:")
for name in models.keys():
    print(f"   - {name}")

results = []

for name, model in models.items():
    print(f"\n{'='*80}")
    print(f"Entraînement: {name}")
    print(f"{'='*80}")
    
    # Mesurer le temps d'entraînement
    start_time = time.time()
    
    # Entraîner
    model.fit(X_train, y_train)
    
    training_time = time.time() - start_time
    
    # Prédictions
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    
    # Métriques Train
    train_mae = mean_absolute_error(y_train, y_train_pred)
    train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
    train_r2 = r2_score(y_train, y_train_pred)
    
    # Métriques Test
    test_mae = mean_absolute_error(y_test, y_test_pred)
    test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
    test_r2 = r2_score(y_test, y_test_pred)
    
    # Cross-validation (optionnel mais recommandé)
    cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='r2')
    cv_mean = cv_scores.mean()
    cv_std = cv_scores.std()

    # Sauvegarder les résultats
    results.append({
        'Model': name,
        'Train_R2': train_r2,
        'Test_R2': test_r2,
        'Train_MAE': train_mae,
        'Test_MAE': test_mae,
        'Train_RMSE': train_rmse,
        'Test_RMSE': test_rmse,
        'CV_R2_Mean': cv_mean,
        'CV_R2_Std': cv_std,
        'Training_Time': training_time,
        'Overfitting': train_r2 - test_r2
    })
# %%
print("\n" + "=" * 80)
print("📊 TABLEAU COMPARATIF DES MODÈLES")
print("=" * 80)

results_df = pd.DataFrame(results)
results_df = results_df.sort_values('Test_R2', ascending=False)

print("\nClassement par R² sur Test:")
print(results_df[['Model', 'Test_R2', 'Test_MAE', 'Test_RMSE', 'Training_Time']].to_string(index=False))

print("\nOverfitting (Train R² - Test R²):")
print(results_df[['Model', 'Train_R2', 'Test_R2', 'Overfitting']].to_string(index=False))

# Meilleur modèle
best_model_name = results_df.iloc[0]['Model']
best_r2 = results_df.iloc[0]['Test_R2']
best_mae = results_df.iloc[0]['Test_MAE']

print(f"MEILLEUR MODÈLE: {best_model_name}")
print(f"   R² Test: {best_r2:.4f}")
print(f"   MAE Test: {best_mae:,.0f} FCFA")
# %%

print("\n" + "=" * 80)
print("Modele prediction avec standardScaler")
print("=" * 80)
df_ss = pd.DataFrame(StandardScaler().fit_transform(data), columns=data.columns)

# %%
print(df_ss.head())
# %%
X = df_ss.drop(columns=['price'])
y = df_ss['price']
# %%
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# %%
models = {
    'Linear Regression': LinearRegression(),
    'Ridge Regression': Ridge(alpha=10.0),
    'Lasso Regression': Lasso(alpha=0.1, max_iter=10000),
    'Decision Tree': DecisionTreeRegressor(max_depth=15, min_samples_split=10, random_state=42),
    'Random Forest': RandomForestRegressor(n_estimators=100, max_depth=20, min_samples_split=5, random_state=42),
    'Gradient Boosting': GradientBoostingRegressor(n_estimators=100, max_depth=5, learning_rate=0.1, random_state=42)   
}
# %%
# %%
print(f"{len(models)} modèles initialisés:")
for name in models.keys():
    print(f"   - {name}")

results = []

for name, model in models.items():
    print(f"\n{'='*80}")
    print(f"Entraînement: {name}")
    print(f"{'='*80}")
    
    # Mesurer le temps d'entraînement
    start_time = time.time()
    
    # Entraîner
    model.fit(X_train, y_train)
    
    training_time = time.time() - start_time
    
    # Prédictions
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    
    # Métriques Train
    train_mae = mean_absolute_error(y_train, y_train_pred)
    train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
    train_r2 = r2_score(y_train, y_train_pred)
    
    # Métriques Test
    test_mae = mean_absolute_error(y_test, y_test_pred)
    test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
    test_r2 = r2_score(y_test, y_test_pred)
    
    # Cross-validation (optionnel mais recommandé)
    cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='r2')
    cv_mean = cv_scores.mean()
    cv_std = cv_scores.std()

    # Sauvegarder les résultats
    results.append({
        'Model': name,
        'Train_R2': train_r2,
        'Test_R2': test_r2,
        'Train_MAE': train_mae,
        'Test_MAE': test_mae,
        'Train_RMSE': train_rmse,
        'Test_RMSE': test_rmse,
        'CV_R2_Mean': cv_mean,
        'CV_R2_Std': cv_std,
        'Training_Time': training_time,
        'Overfitting': train_r2 - test_r2
    })

# %%
# %%
print("\n" + "=" * 80)
print("📊 TABLEAU COMPARATIF DES MODÈLES")
print("=" * 80)

results_df = pd.DataFrame(results)
results_df = results_df.sort_values('Test_R2', ascending=False)

print("\nClassement par R² sur Test:")
print(results_df[['Model', 'Test_R2', 'Test_MAE', 'Test_RMSE', 'Training_Time']].to_string(index=False))

print("\nOverfitting (Train R² - Test R²):")
print(results_df[['Model', 'Train_R2', 'Test_R2', 'Overfitting']].to_string(index=False))

# Meilleur modèle
best_model_name = results_df.iloc[0]['Model']
best_r2 = results_df.iloc[0]['Test_R2']
best_mae = results_df.iloc[0]['Test_MAE']

print(f"MEILLEUR MODÈLE: {best_model_name}")
print(f"   R² Test: {best_r2:.4f}")
print(f"   MAE Test: {best_mae:,.0f} FCFA")
# %%
