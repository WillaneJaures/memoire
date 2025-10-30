# dags/train_model_dag.py

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='train_ml_model',
    start_date=datetime(2025, 10, 25),
    schedule='@Weekly',  # ✅ Schedule quotidien
    catchup=False,
    default_args=default_args,
    description="DAG pour entraîner le modèle ML après chargement des données"
) as dag:

    # ============================
    # SENSOR : Attendre la fin de l'ETL
    # ============================
    wait_for_etl = ExternalTaskSensor(
        task_id='wait_for_data_loading',
        external_dag_id='coindakaretl',
        external_task_id='upload_join_to_sqlite',
        execution_delta=None,  
        timeout=7200,  # 2 heures max
        mode='reschedule',  # ✅ Libère le worker pendant l'attente
        poke_interval=300  # Vérifie toutes les 5 minutes
    )

    @task
    def train_model():
        """Entraîner le modèle ML et sauvegarder les artefacts"""
        import pandas as pd
        import numpy as np
        import sqlite3
        import joblib
        import os
        from sklearn.model_selection import train_test_split, GridSearchCV
        from sklearn.preprocessing import StandardScaler, OneHotEncoder
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
        
        logging.info("="*80)
        logging.info("🚀 DÉBUT DE L'ENTRAÎNEMENT DU MODÈLE")
        logging.info("="*80)
        
        # ============================
        # 1️⃣ CHARGER LES DONNÉES
        # ============================
        db_path = '../data/immobilier.db'
        conn = sqlite3.connect(db_path)
        df = pd.read_sql("SELECT * FROM realestate", conn)
        conn.close()
        
        logging.info(f"✅ Données chargées: {df.shape}")
        
        # ============================
        # 2️⃣ PREPROCESSING
        # ============================
        df = df.drop(columns=['id', 'source'], errors='ignore')
        df = df.drop_duplicates()
        
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        strings = list(df.dtypes[df.dtypes == 'object'].index)
        for col in strings:
            df[col] = df[col].str.lower().str.replace(' ', '_')
        
        df = df[~df['type'].isin(['unknown', 'immobilier'])]
        
        logging.info(f"✅ Nettoyage terminé: {df.shape}")
        
        # ============================
        # 3️⃣ FEATURE ENGINEERING
        # ============================
        df['prix_par_m2'] = df['price'] / (df['superficie'] + 1)
        df['ratio_sdb_chambres'] = df['nombre_sdb'] / (df['nombre_chambres'] + 1)
        df['surface_par_chambre'] = df['superficie'] / (df['nombre_chambres'] + 1)
        df['log_superficie'] = np.log1p(df['superficie'])
        
        logging.info("✅ Feature engineering terminé")
        
        # ============================
        # 4️⃣ ENCODAGE
        # ============================
        categorical_cols = ['type', 'category', 'area', 'city']
        numerical_cols = ['superficie', 'nombre_chambres', 'nombre_sdb', 
                          'prix_par_m2', 'ratio_sdb_chambres', 'surface_par_chambre',
                          'log_superficie']
        
        X_num = df[numerical_cols]
        X_cat = df[categorical_cols]
        
        encoder = OneHotEncoder(sparse_output=False, drop='first', handle_unknown='ignore')
        X_cat_encoded = encoder.fit_transform(X_cat)
        feature_names = encoder.get_feature_names_out(categorical_cols)
        
        X_cat_df = pd.DataFrame(X_cat_encoded, columns=feature_names, index=df.index)
        X = pd.concat([X_num.reset_index(drop=True), X_cat_df.reset_index(drop=True)], axis=1)
        y = df['price'].reset_index(drop=True)
        
        logging.info(f"✅ Encodage terminé: {X.shape}")
        
        # ============================
        # 5️⃣ SPLIT & SCALING
        # ============================
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        logging.info(f"✅ Split: Train={X_train.shape}, Test={X_test.shape}")
        
        # ============================
        # 6️⃣ ENTRAÎNEMENT AVEC GRID SEARCH
        # ============================
        logging.info("🔄 Grid Search en cours...")
        
        param_grid = {
            'n_estimators': [200, 300],
            'max_depth': [4, 5],
            'learning_rate': [0.05, 0.07],
            'subsample': [0.8],
            'min_samples_split': [20],
            'min_samples_leaf': [10]
        }
        
        base_model = GradientBoostingRegressor(random_state=42)
        
        grid_search = GridSearchCV(
            base_model, 
            param_grid, 
            cv=5, 
            scoring='r2',
            n_jobs=-1,
            verbose=2
        )
        
        grid_search.fit(X_train_scaled, y_train)
        
        best_model = grid_search.best_estimator_
        
        logging.info(f"✅ Meilleurs paramètres: {grid_search.best_params_}")
        logging.info(f"✅ Meilleur score CV: {grid_search.best_score_:.4f}")
        
        # ============================
        # 7️⃣ ÉVALUATION
        # ============================
        y_train_pred = best_model.predict(X_train_scaled)
        y_test_pred = best_model.predict(X_test_scaled)
        
        train_r2 = r2_score(y_train, y_train_pred)
        test_r2 = r2_score(y_test, y_test_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
        
        metrics = {
            'train_r2': float(train_r2),
            'test_r2': float(test_r2),
            'test_mae': float(test_mae),
            'test_rmse': float(test_rmse),
            'overfitting': float(train_r2 - test_r2),
            'best_params': grid_search.best_params_,
            'timestamp': datetime.now().isoformat()
        }
        
        logging.info("="*80)
        logging.info(" RÉSULTATS FINAUX")
        logging.info("="*80)
        logging.info(f"Train R²: {train_r2:.4f}")
        logging.info(f"Test R²: {test_r2:.4f}")
        logging.info(f"MAE: {test_mae:,.0f} FCFA")
        logging.info(f"RMSE: {test_rmse:,.0f} FCFA")
        logging.info(f"Overfitting: {train_r2 - test_r2:.4f}")
        
        # ============================
        # 8️⃣ SAUVEGARDER LES ARTEFACTS
        # ============================
        models_dir = '/usr/local/airflow/data/models'
        os.makedirs(models_dir, exist_ok=True)
        
        joblib.dump(best_model, f'{models_dir}/best_model.pkl')
        joblib.dump(encoder, f'{models_dir}/encoder.pkl')
        joblib.dump(scaler, f'{models_dir}/scaler.pkl')
        joblib.dump(metrics, f'{models_dir}/metrics.pkl')
        
        joblib.dump({
            'numerical_cols': numerical_cols,
            'categorical_cols': categorical_cols,
            'feature_names': list(feature_names)
        }, f'{models_dir}/columns.pkl')
        
        logging.info(f"✅ Modèle sauvegardé dans {models_dir}")
        logging.info("="*80)
        
        return metrics

    @task
    def validate_model():
        """Valider que le modèle fonctionne correctement"""
        import joblib
        import numpy as np
        import pandas as pd
        
        models_dir = '/usr/local/airflow/models'
        
        model = joblib.load(f'{models_dir}/best_model.pkl')
        encoder = joblib.load(f'{models_dir}/encoder.pkl')
        scaler = joblib.load(f'{models_dir}/scaler.pkl')
        columns = joblib.load(f'{models_dir}/columns.pkl')
        
        logging.info("✅ Modèle chargé avec succès")
        
        test_data = pd.DataFrame({
            'superficie': [150],
            'nombre_chambres': [3],
            'nombre_sdb': [2],
            'prix_par_m2': [5000],
            'ratio_sdb_chambres': [0.67],
            'surface_par_chambre': [50],
            'log_superficie': [np.log1p(150)],
            'type': ['appartements'],
            'category': ['location'],
            'area': ['almadies'],
            'city': ['dakar']
        })
        
        X_num = test_data[columns['numerical_cols']]
        X_cat = test_data[columns['categorical_cols']]
        
        X_cat_encoded = encoder.transform(X_cat)
        X_cat_df = pd.DataFrame(X_cat_encoded, columns=columns['feature_names'])
        
        X = pd.concat([X_num.reset_index(drop=True), X_cat_df.reset_index(drop=True)], axis=1)
        X_scaled = scaler.transform(X)
        
        prediction = model.predict(X_scaled)[0]
        
        logging.info(f"✅ Test de prédiction: {prediction:,.0f} FCFA")
        logging.info(f"✅ Modèle validé et prêt pour la production")
        
        return True

    @task
    def notify_training_complete(metrics):
        """Notifier que l'entraînement est terminé"""
        logging.info("="*80)
        logging.info("✅ ENTRAÎNEMENT TERMINÉ AVEC SUCCÈS")
        logging.info("="*80)
        logging.info(f"Performance finale: R² = {metrics['test_r2']:.4f}")
        logging.info(f"MAE: {metrics['test_mae']:,.0f} FCFA")
        logging.info(f"Timestamp: {metrics['timestamp']}")
        logging.info("🎉 Le modèle est prêt à être utilisé dans Streamlit")
        logging.info("="*80)
        
        return True

    # ============================
    # ORCHESTRATION
    # ============================
    training_results = train_model()
    validation = validate_model()
    notification = notify_training_complete(training_results)
    
    # Flux : Attendre l'ETL → Entraîner → Valider → Notifier
    wait_for_etl >> training_results >> validation >> notification