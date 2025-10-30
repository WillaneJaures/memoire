import pandas as pd
import numpy as np
import sqlite3
import joblib
import os
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from datetime import datetime
import logging
import json

def train_ml_model():
    """
    Entraîne le modèle ML sans data leakage avec features engineering avancé
    """
    logging.info("🚀 Début de l'entraînement du modèle ML optimisé")
    
    # ============================
    # 1️⃣ CHARGEMENT DES DONNÉES
    # ============================
    db_path = '../data/immobilier.db'
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"❌ Base SQLite introuvable : {db_path}")
    
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM realestate", conn)
    conn.close()
    
    if df.empty:
        raise ValueError("❌ Aucune donnée trouvée")
    
    logging.info(f"✅ Données chargées : {df.shape}")
    
    # ============================
    # 2️⃣ NETTOYAGE DES DONNÉES
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
    string_cols = list(df.dtypes[df.dtypes == 'object'].index)
    for col in string_cols:
        df[col] = df[col].str.lower().str.replace(' ', '_')
    
    # Renommer valeurs
    df['type'] = df['type'].replace({
        'appartement': 'appartements',
        'villa': 'villas',
    })
    
    logging.info(f"✅ Nettoyage terminé : {df.shape}")
    
    # ============================
    # 3️⃣ TRAITEMENT DES OUTLIERS
    # ============================
    
    # Corriger superficies aberrantes
    median_superficie = df['superficie'].median()
    df.loc[df['superficie'] < 20, 'superficie'] = median_superficie
    
    # Traiter outliers avec IQR
    def impute_outliers(df, feature):
        q1 = np.percentile(df[feature], 25)
        q3 = np.percentile(df[feature], 75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        df.loc[df[feature] < lower_bound, feature] = lower_bound
        df.loc[df[feature] > upper_bound, feature] = upper_bound
    
    for feature in ['price', 'superficie', 'nombre_chambres', 'nombre_sdb']:
        impute_outliers(df, feature)
    
    logging.info("✅ Outliers traités")
    
    # ============================
    # 4️⃣ FEATURE ENGINEERING (SANS DATA LEAKAGE)
    # ============================
    
    # ✅ Features légitimes (n'utilisent PAS la cible price)
    
    # 1. Ratios et interactions entre features
    df['ratio_sdb_chambres'] = df['nombre_sdb'] / (df['nombre_chambres'] + 1)
    df['surface_par_chambre'] = df['superficie'] / (df['nombre_chambres'] + 1)
    df['total_pieces'] = df['nombre_chambres'] + df['nombre_sdb']
    df['density'] = df['nombre_chambres'] / (df['superficie'] + 1)  # Chambres/m²
    
    # 2. Transformations non-linéaires
    df['log_superficie'] = np.log1p(df['superficie'])
    df['sqrt_superficie'] = np.sqrt(df['superficie'])
    df['superficie_squared'] = df['superficie'] ** 2
    
    # 3. Features binaires géographiques
    premium_areas = ['almadies', 'ngor', 'mermoz', 'sacré-coeur', 'fann']
    df['is_premium_area'] = df['area'].isin(premium_areas).astype(int)
    df['is_dakar'] = (df['city'] == 'dakar').astype(int)
    
    # 4. Features catégorielles enrichies
    df['is_villa'] = (df['type'] == 'villas').astype(int)
    df['is_location'] = (df['category'] == 'location').astype(int)
    
    # 5. Interactions type × taille
    df['villa_large'] = ((df['type'] == 'villas') & (df['superficie'] > 200)).astype(int)
    df['appt_petit'] = ((df['type'] == 'appartements') & (df['superficie'] < 80)).astype(int)
    
    # 6. Features de luxe
    df['high_bathroom_ratio'] = (df['nombre_sdb'] >= df['nombre_chambres']).astype(int)
    df['spacious'] = (df['surface_par_chambre'] > 40).astype(int)
    
    logging.info("✅ Feature engineering terminé (sans data leakage)")
    
    # ============================
    # 5️⃣ PRÉPARATION DES FEATURES
    # ============================
    
    # Features numériques (n'incluent PAS price ou log_price)
    numerical_cols = [
        'superficie',
        'nombre_chambres',
        'nombre_sdb',
        'ratio_sdb_chambres',
        'surface_par_chambre',
        'total_pieces',
        'density',
        'log_superficie',
        'sqrt_superficie',
        'superficie_squared',
        'is_premium_area',
        'is_dakar',
        'is_villa',
        'is_location',
        'villa_large',
        'appt_petit',
        'high_bathroom_ratio',
        'spacious'
    ]
    
    # Features catégorielles
    categorical_cols = ['type', 'category', 'area', 'city']
    
    X_num = df[numerical_cols]
    X_cat = df[categorical_cols]
    
    # ============================
    # 6️⃣ ENCODAGE ONE-HOT
    # ============================
    
    encoder = OneHotEncoder(sparse_output=False, drop='first', handle_unknown='ignore')
    X_cat_encoded = encoder.fit_transform(X_cat)
    feature_names = encoder.get_feature_names_out(categorical_cols)
    
    X_cat_df = pd.DataFrame(
        X_cat_encoded,
        columns=feature_names,
        index=df.index
    )
    
    # Combiner features numériques et catégorielles
    X = pd.concat([
        X_num.reset_index(drop=True),
        X_cat_df.reset_index(drop=True)
    ], axis=1)
    
    y = df['price'].reset_index(drop=True)
    
    logging.info(f"✅ Features préparées : {X.shape}")
    
    # Sauvegarder info preprocessing
    preprocessing_info = {
        'numerical_cols': numerical_cols,
        'categorical_cols': categorical_cols,
        'feature_names': feature_names.tolist(),
        'all_columns': X.columns.tolist()
    }
    
    # ============================
    # 7️⃣ SPLIT TRAIN/TEST
    # ============================
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=df['category'].reset_index(drop=True)
    )
    
    logging.info(f"✅ Split : Train={X_train.shape}, Test={X_test.shape}")
    
    # ============================
    # 8️⃣ ENTRAÎNEMENT DES MODÈLES
    # ============================
    
    models = {
        'GradientBoosting': GradientBoostingRegressor(
            n_estimators=400,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            min_samples_split=20,
            min_samples_leaf=10,
            random_state=42,
            verbose=0
        ),
        'RandomForest': RandomForestRegressor(
            n_estimators=300,
            max_depth=15,
            min_samples_split=20,
            min_samples_leaf=10,
            max_features='sqrt',
            random_state=42,
            n_jobs=-1,
            verbose=0
        )
    }
    
    results = {}
    best_model = None
    best_score = -np.inf
    best_model_name = None
    
    for model_name, model in models.items():
        logging.info(f"🔄 Entraînement : {model_name}")
        
        # Entraînement
        model.fit(X_train, y_train)
        
        # Prédictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Métriques
        train_r2 = r2_score(y_train, y_train_pred)
        test_r2 = r2_score(y_test, y_test_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
        
        # Cross-validation
        cv_scores = cross_val_score(
            model, X_train, y_train,
            cv=5, scoring='r2', n_jobs=-1
        )
        
        # Sauvegarder résultats
        results[model_name] = {
            'train_r2': float(train_r2),
            'test_r2': float(test_r2),
            'test_mae': float(test_mae),
            'test_rmse': float(test_rmse),
            'cv_mean': float(cv_scores.mean()),
            'cv_std': float(cv_scores.std()),
            'overfitting': float(train_r2 - test_r2)
        }
        
        logging.info(
            f"  {model_name} - Train R²: {train_r2:.4f} | "
            f"Test R²: {test_r2:.4f} | Écart: {train_r2-test_r2:.4f}"
        )
        
        # Sélectionner le meilleur modèle
        if test_r2 > best_score:
            best_score = test_r2
            best_model = model
            best_model_name = model_name
    
    # ============================
    # 9️⃣ ANALYSE DES FEATURES
    # ============================
    
    # Importance des features pour le meilleur modèle
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': best_model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    top_features = feature_importance.head(10)
    logging.info("\n📊 Top 10 features importantes:")
    for idx, row in top_features.iterrows():
        logging.info(f"  {row['feature']}: {row['importance']:.4f}")
    
    # ============================
    # 🔟 SAUVEGARDE DES MODÈLES
    # ============================
    
    models_dir = '/usr/local/airflow/data/models'
    os.makedirs(models_dir, exist_ok=True)
    
    # Sauvegarder le meilleur modèle
    joblib.dump(best_model, f'{models_dir}/best_model.pkl')
    joblib.dump(encoder, f'{models_dir}/encoder.pkl')
    joblib.dump(preprocessing_info, f'{models_dir}/preprocessing_info.pkl')
    joblib.dump(feature_importance, f'{models_dir}/feature_importance.pkl')
    
    # Sauvegarder les résultats
    final_metrics = {
        'best_model_name': best_model_name,
        'results': results,
        'timestamp': datetime.now().isoformat(),
        'training_samples': len(X_train),
        'test_samples': len(X_test),
        'total_features': X.shape[1]
    }
    
    joblib.dump(final_metrics, f'{models_dir}/metrics.pkl')
    
    # ============================
    # 1️⃣1️⃣ RAPPORT FINAL
    # ============================
    
    best_results = results[best_model_name]
    
    logging.info("\n" + "="*80)
    logging.info(f"✅ MEILLEUR MODÈLE : {best_model_name}")
    logging.info("="*80)
    logging.info(f"📊 Train R² : {best_results['train_r2']:.4f}")
    logging.info(f"📊 Test R²  : {best_results['test_r2']:.4f}")
    logging.info(f"📊 CV R²    : {best_results['cv_mean']:.4f} (±{best_results['cv_std']:.4f})")
    logging.info(f"📊 MAE      : {best_results['test_mae']:,.0f} FCFA")
    logging.info(f"📊 RMSE     : {best_results['test_rmse']:,.0f} FCFA")
    logging.info(f"📊 Overfitting : {best_results['overfitting']:.4f}")
    
    if best_results['overfitting'] < 0.05:
        logging.info("✅ Excellent - Pas d'overfitting")
    elif best_results['overfitting'] < 0.10:
        logging.info("✅ Bon - Overfitting minimal")
    else:
        logging.info("⚠️ Overfitting modéré")
    
    logging.info(f"💾 Modèles sauvegardés dans {models_dir}")
    logging.info("="*80)
    
    # Retourner les métriques pour Airflow
    return {
        'model_name': best_model_name,
        'train_r2': best_results['train_r2'],
        'test_r2': best_results['test_r2'],
        'mae': best_results['test_mae'],
        'rmse': best_results['test_rmse'],
        'cv_mean': best_results['cv_mean'],
        'cv_std': best_results['cv_std'],
        'overfitting': best_results['overfitting'],
        'timestamp': final_metrics['timestamp']
    }


# Pour tester localement
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    metrics = train_ml_model()
    print("\n🎯 Résultats finaux:")
    print(json.dumps(metrics, indent=2, default=str))