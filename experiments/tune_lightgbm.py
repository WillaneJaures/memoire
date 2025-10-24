# %%
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import sqlite3 
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, ExtraTreesRegressor
from sklearn.tree import DecisionTreeRegressor
import time
import warnings
warnings.filterwarnings('ignore')

# %%
# ============================
# CHARGEMENT DES DONNÉES
# ============================
db_path = '../data/immobilier.db'
conn = sqlite3.connect(db_path)
df = pd.read_sql("SELECT * FROM realestate", conn)
conn.close()

print(" Aperçu des données :")
print(df.head())
print(f"Shape: {df.shape}")

# %%
# ============================
#  NETTOYAGE DES DONNÉES
# ============================

# Supprimer colonnes inutiles
df = df.drop(columns=['id', 'source'], errors='ignore')

# Supprimer doublons
df = df.drop_duplicates()

# Uniformiser noms de colonnes
df.columns = df.columns.str.lower().str.replace(' ', '_')

# Supprimer valeurs aberrantes dans type
df = df[~df['type'].isin(['Unknown', 'Immobilier', 'unknown', 'immobilier'])]

# Uniformiser valeurs catégorielles
strings = list(df.dtypes[df.dtypes == 'object'].index)
for col in strings:
    df[col] = df[col].str.lower().str.replace(' ', '_')

# Renommer valeurs
df['type'] = df['type'].replace({
    'appartement': 'appartements',
    'villa': 'villas',
})

print(f"\n Nettoyage terminé. Shape: {df.shape}")

# %%
# ============================
#  TRAITEMENT DES OUTLIERS
# ============================

# Corriger superficies aberrantes
median_superficie = df['superficie'].median()
df.loc[df['superficie'] < 20, 'superficie'] = median_superficie

# Traiter outliers avec IQR
features = ['price', 'superficie', 'nombre_chambres', 'nombre_sdb']

def impute_outliers(df, feature):
    q1 = np.percentile(df[feature], 25)
    q3 = np.percentile(df[feature], 75) 
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    df.loc[df[feature] < lower_bound, feature] = lower_bound
    df.loc[df[feature] > upper_bound, feature] = upper_bound

for feature in features:
    impute_outliers(df, feature)

print("Outliers traités")

# %%
# ============================
#  FEATURE ENGINEERING
# ============================

# Créer nouvelles features
df['prix_par_m2'] = df['price'] / (df['superficie'] + 1)  # +1 pour éviter division par 0
df['ratio_sdb_chambres'] = df['nombre_sdb'] / (df['nombre_chambres'] + 1)
df['surface_par_chambre'] = df['superficie'] / (df['nombre_chambres'] + 1)

# Log transform pour les distributions asymétriques
df['log_price'] = np.log1p(df['price'])
df['log_superficie'] = np.log1p(df['superficie'])

print("Feature engineering terminé")
print(f"Nouvelles colonnes: {df.columns.tolist()}")

# %%
# ============================
# ENCODAGE ONEHOT
# ============================

categorical_cols = ['type', 'category', 'area', 'city']
numerical_cols = ['superficie', 'nombre_chambres', 'nombre_sdb', 
                  'prix_par_m2', 'ratio_sdb_chambres', 'surface_par_chambre',
                  'log_superficie']

X_num = df[numerical_cols]
X_cat = df[categorical_cols]

# OneHotEncoder
encoder = OneHotEncoder(sparse_output=False, drop='first', handle_unknown='ignore')
X_cat_encoded = encoder.fit_transform(X_cat)
feature_names = encoder.get_feature_names_out(categorical_cols)

X_cat_df = pd.DataFrame(X_cat_encoded, columns=feature_names, index=df.index)
X = pd.concat([X_num.reset_index(drop=True), X_cat_df.reset_index(drop=True)], axis=1)
y = df['price'].reset_index(drop=True)  # Garder y non normalisé

print(f"\n Features encodées: {X.shape}")

# %%
# ============================
# SPLIT TRAIN/TEST
# ============================

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=df['category'].reset_index(drop=True)
)

print(f" Split:")
print(f"  Train: {X_train.shape}")
print(f"  Test: {X_test.shape}")

# %%
# ============================
#  FEATURE SCALING
# ============================

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

print(" Scaling effectué")

# %%
# ============================
#  MODÈLES OPTIMISÉS
# ============================

print("\n" + "="*80)
print("ENTRAÎNEMENT DES MODÈLES OPTIMISÉS")
print("="*80)

# Modèles avec scaling (régularisation)
models_scaled = {
    'Ridge (Optimisé)': Ridge(
        alpha=50.0,  # Augmenté pour réduire overfitting
        max_iter=10000
    ),
    
    'Lasso (Optimisé)': Lasso(
        alpha=500.0,  #  Augmenté
        max_iter=10000
    ),
    
    'ElasticNet': ElasticNet(
        alpha=100.0,
        l1_ratio=0.5,  # Mix L1 et L2
        max_iter=10000
    ),
}

# Modèles sans scaling (arbres optimisés)
models_no_scale = {
    'Random Forest (Optimisé)': RandomForestRegressor(
        n_estimators=200,        #  Augmenté de 100 à 200
        max_depth=12,            #  Réduit de 20 à 12
        min_samples_split=20,    #  Augmenté de 5 à 20
        min_samples_leaf=10,     #  Nouvelle contrainte
        max_features='sqrt',     # Limite les features
        random_state=42,
        n_jobs=-1                # Utiliser tous les CPU
    ),
    
    'Gradient Boosting (Optimisé)': GradientBoostingRegressor(
        n_estimators=300,        #  Augmenté
        max_depth=4,             #  Réduit de 5 à 4
        learning_rate=0.05,      #  Réduit de 0.1 à 0.05
        subsample=0.8,           #  Sous-échantillonnage
        min_samples_split=20,
        min_samples_leaf=10,
        random_state=42
    ),
    
    'Extra Trees': ExtraTreesRegressor(
        n_estimators=200,
        max_depth=12,
        min_samples_split=20,
        min_samples_leaf=10,
        max_features='sqrt',
        random_state=42,
        n_jobs=-1
    ),
    
    'Decision Tree (Optimisé)': DecisionTreeRegressor(
        max_depth=10,            #  Réduit de 15 à 10
        min_samples_split=30,    #  Augmenté
        min_samples_leaf=15,     #  Nouvelle contrainte
        random_state=42
    ),
}

# %%
# Entraîner tous les modèles
results = []

# Modèles avec scaling
print("\n Modèles avec scaling:")
for name, model in models_scaled.items():
    print(f"\n   {name}...")
    start_time = time.time()
    
    model.fit(X_train_scaled, y_train)
    training_time = time.time() - start_time
    
    y_train_pred = model.predict(X_train_scaled)
    y_test_pred = model.predict(X_test_scaled)
    
    train_r2 = r2_score(y_train, y_train_pred)
    test_r2 = r2_score(y_test, y_test_pred)
    test_mae = mean_absolute_error(y_test, y_test_pred)
    test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
    
    cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=5, scoring='r2')
    
    results.append({
        'Model': name,
        'Scaling': 'Yes',
        'Train_R2': train_r2,
        'Test_R2': test_r2,
        'Test_MAE': test_mae,
        'Test_RMSE': test_rmse,
        'CV_R2_Mean': cv_scores.mean(),
        'CV_R2_Std': cv_scores.std(),
        'Training_Time': training_time,
        'Overfitting': train_r2 - test_r2
    })
    
    print(f"     Train R²: {train_r2:.4f} | Test R²: {test_r2:.4f} | Écart: {train_r2-test_r2:.4f}")

# %%
# Modèles sans scaling
print("\n Modèles sans scaling:")
for name, model in models_no_scale.items():
    print(f"\n   {name}...")
    start_time = time.time()
    
    model.fit(X_train, y_train)
    training_time = time.time() - start_time
    
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    
    train_r2 = r2_score(y_train, y_train_pred)
    test_r2 = r2_score(y_test, y_test_pred)
    test_mae = mean_absolute_error(y_test, y_test_pred)
    test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
    
    cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='r2')
    
    results.append({
        'Model': name,
        'Scaling': 'No',
        'Train_R2': train_r2,
        'Test_R2': test_r2,
        'Test_MAE': test_mae,
        'Test_RMSE': test_rmse,
        'CV_R2_Mean': cv_scores.mean(),
        'CV_R2_Std': cv_scores.std(),
        'Training_Time': training_time,
        'Overfitting': train_r2 - test_r2
    })
    
    print(f"     Train R²: {train_r2:.4f} | Test R²: {test_r2:.4f} | Écart: {train_r2-test_r2:.4f}")

# %%
# ============================
# 9️⃣ RÉSULTATS COMPARATIFS
# ============================

print("\n" + "="*80)
print(" TABLEAU COMPARATIF DES MODÈLES OPTIMISÉS")
print("="*80)

results_df = pd.DataFrame(results)
results_df = results_df.sort_values('Test_R2', ascending=False)

print("\n Classement par R² Test:")
print(results_df[['Model', 'Scaling', 'Test_R2', 'Test_MAE', 'Overfitting']].to_string(index=False))

print("\n Analyse complète:")
print(results_df[['Model', 'Train_R2', 'Test_R2', 'CV_R2_Mean', 'Overfitting']].to_string(index=False))

# %%
# Meilleur modèle
best_idx = results_df['Test_R2'].idxmax()
best_model_name = results_df.loc[best_idx, 'Model']
best_scaling = results_df.loc[best_idx, 'Scaling']
best_r2 = results_df.loc[best_idx, 'Test_R2']
best_mae = results_df.loc[best_idx, 'Test_MAE']
best_overfitting = results_df.loc[best_idx, 'Overfitting']

print("\n" + "="*80)
print(f" MEILLEUR MODÈLE: {best_model_name}")
print("="*80)
print(f"   Scaling: {best_scaling}")
print(f"   Train R²: {results_df.loc[best_idx, 'Train_R2']:.4f}")
print(f"   Test R²: {best_r2:.4f}")
print(f"   CV R²: {results_df.loc[best_idx, 'CV_R2_Mean']:.4f} (±{results_df.loc[best_idx, 'CV_R2_Std']:.4f})")
print(f"   MAE: {best_mae:,.0f} FCFA")
print(f"   Overfitting: {best_overfitting:.4f}")

if best_overfitting < 0.05:
    print("    Excellent - Pas d'overfitting")
elif best_overfitting < 0.10:
    print("    Bon - Overfitting minimal")
elif best_overfitting < 0.15:
    print("    Overfitting modéré")
else:
    print("    Overfitting important")

# %%
# ============================
#  VISUALISATIONS
# ============================

fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle(' Comparaison des Modèles Optimisés', fontsize=16, fontweight='bold')

# R² Score
ax1 = axes[0, 0]
x_pos = np.arange(len(results_df))
width = 0.35
ax1.bar(x_pos - width/2, results_df['Train_R2'], width, label='Train R²', color='skyblue', edgecolor='black')
ax1.bar(x_pos + width/2, results_df['Test_R2'], width, label='Test R²', color='coral', edgecolor='black')
ax1.set_xlabel('Modèles')
ax1.set_ylabel('R² Score')
ax1.set_title(' R² Score (Train vs Test)')
ax1.set_xticks(x_pos)
ax1.set_xticklabels(results_df['Model'], rotation=45, ha='right', fontsize=8)
ax1.legend()
ax1.grid(axis='y', alpha=0.3)

# Overfitting
ax2 = axes[0, 1]
colors = ['green' if x < 0.05 else 'orange' if x < 0.10 else 'red' for x in results_df['Overfitting']]
ax2.bar(x_pos, results_df['Overfitting'], color=colors, edgecolor='black')
ax2.axhline(y=0.05, color='green', linestyle='--', linewidth=2, label='Excellent')
ax2.axhline(y=0.10, color='orange', linestyle='--', linewidth=2, label='Acceptable')
ax2.set_xlabel('Modèles')
ax2.set_ylabel('Écart Train-Test R²')
ax2.set_title(' Overfitting (plus bas = mieux)')
ax2.set_xticks(x_pos)
ax2.set_xticklabels(results_df['Model'], rotation=45, ha='right', fontsize=8)
ax2.legend()
ax2.grid(axis='y', alpha=0.3)

# MAE
ax3 = axes[1, 0]
ax3.bar(x_pos, results_df['Test_MAE'], color='lightgreen', edgecolor='black')
ax3.set_xlabel('Modèles')
ax3.set_ylabel('MAE (FCFA)')
ax3.set_title(' Mean Absolute Error')
ax3.set_xticks(x_pos)
ax3.set_xticklabels(results_df['Model'], rotation=45, ha='right', fontsize=8)
ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
ax3.grid(axis='y', alpha=0.3)

# Cross-validation
ax4 = axes[1, 1]
ax4.bar(x_pos, results_df['CV_R2_Mean'], yerr=results_df['CV_R2_Std'], 
        color='plum', edgecolor='black', capsize=5)
ax4.set_xlabel('Modèles')
ax4.set_ylabel('R² (Cross-validation)')
ax4.set_title(' R² avec Cross-Validation (5-fold)')
ax4.set_xticks(x_pos)
ax4.set_xticklabels(results_df['Model'], rotation=45, ha='right', fontsize=8)
ax4.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.show()

# %%
# ============================
#  GRID SEARCH POUR LE MEILLEUR MODÈLE
# ============================

print("\n" + "="*80)
print(" GRID SEARCH SUR LE MEILLEUR MODÈLE")
print("="*80)

if 'Random Forest' in best_model_name or 'Gradient Boosting' in best_model_name:
    if 'Random Forest' in best_model_name:
        param_grid = {
            'n_estimators': [200, 300],
            'max_depth': [10, 12, 15],
            'min_samples_split': [15, 20, 25],
            'min_samples_leaf': [8, 10, 12]
        }
        base_model = RandomForestRegressor(random_state=42, n_jobs=-1)
    else:
        param_grid = {
            'n_estimators': [200, 300, 400],
            'max_depth': [3, 4, 5],
            'learning_rate': [0.03, 0.05, 0.07],
            'subsample': [0.7, 0.8, 0.9]
        }
        base_model = GradientBoostingRegressor(random_state=42)
    
    print(f"\n Grid Search en cours sur {best_model_name}...")
    print(f"Paramètres testés: {param_grid}")
    
    grid_search = GridSearchCV(
        base_model, 
        param_grid, 
        cv=5, 
        scoring='r2',
        n_jobs=-1,
        verbose=1
    )
    
    if best_scaling == 'Yes':
        grid_search.fit(X_train_scaled, y_train)
    else:
        grid_search.fit(X_train, y_train)
    
    print(f"\n Meilleurs paramètres trouvés:")
    print(grid_search.best_params_)
    print(f"\n Meilleur score CV: {grid_search.best_score_:.4f}")
    
    # Évaluer sur test
    if best_scaling == 'Yes':
        y_pred_grid = grid_search.predict(X_test_scaled)
    else:
        y_pred_grid = grid_search.predict(X_test)
    
    test_r2_grid = r2_score(y_test, y_pred_grid)
    test_mae_grid = mean_absolute_error(y_test, y_pred_grid)
    
    print(f" Score Test après Grid Search: {test_r2_grid:.4f}")
    print(f" MAE Test: {test_mae_grid:,.0f} FCFA")
    
    print(f"\n Amélioration:")
    print(f"   R² avant: {best_r2:.4f}")
    print(f"   R² après: {test_r2_grid:.4f}")
    print(f"   Gain: {(test_r2_grid - best_r2):.4f} ({((test_r2_grid - best_r2)/best_r2*100):.2f}%)")

# %%
print("\n" + "="*80)
print(" OPTIMISATION TERMINÉE")
print("="*80)

# %%