from sklearn.preprocessing import OneHotEncoder

categorical_cols = ['type', 'category', 'area', 'city']
numerical_cols = ['superficie', 'nombre_chambres', 'nombre_sdb']
# %%
X_num = df[numerical_cols]
X_cat = df[categorical_cols]
# %%
#One hot encoding
encoder = OneHotEncoder(sparse_output=False, drop='first', handle_unknown='ignore')
X_cat_encoded = encoder.fit_transform(X_cat)
# %%
feature_names = encoder.get_feature_names_out(categorical_cols)
print(f"\nFeatures créées: {len(feature_names)}")
print(f"Exemples: {feature_names[:10].tolist()}")
# %%
X_cat_df = pd.DataFrame(X_cat_encoded, columns=feature_names, index=df.index)
# %%
X_final = pd.concat([X_num, X_cat_df], axis=1)
y = df['price']
# %%
print(X_final.head())
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import time


from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.tree import DecisionTreeRegressor


X_train, X_test, y_train, y_test = train_test_split(X_final, y, test_size=0.2, random_state=42)
print(f"Train set size: {X_train.shape}, Test set size: {X_test.shape}")
# %%
models = {
    'Linear Regression': LinearRegression(),
    'Ridge Regression': Ridge(alpha=10.0),
    'Lasso Regression': Lasso(alpha=100.0),
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
    print(f"🔄 Entraînement: {name}")
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
    
    # Afficher les résultats
    print(f"\nRésultats {name}:")
    print(f"   Temps d'entraînement: {training_time:.2f}s")
    print(f"\n   TRAIN:")
    print(f"      MAE:  {train_mae:,.0f} FCFA")
    print(f"      RMSE: {train_rmse:,.0f} FCFA")
    print(f"      R²:   {train_r2:.4f}")
    print(f"\n   TEST:")
    print(f"      MAE:  {test_mae:,.0f} FCFA")
    print(f"      RMSE: {test_rmse:,.0f} FCFA")
    print(f"      R²:   {test_r2:.4f}")
    print(f"\n   CROSS-VALIDATION (5-fold):")
    print(f"      R² moyen: {cv_mean:.4f} (±{cv_std:.4f})")
    print(f"\n   OVERFITTING:")
    print(f"      Δ R² (train - test): {train_r2 - test_r2:.4f}")
    
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
