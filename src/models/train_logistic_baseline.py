"""
train_logistic_baseline.py - Train and understand a logistic regression model

This script emphasizes interpretability and understanding:
1. What logistic regression is and why it works
2. How to interpret coefficients 
3. Which features matter most
4. Where the model succeeds and fails
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import (
    accuracy_score, confusion_matrix, classification_report,
    roc_auc_score, roc_curve, log_loss
)
import joblib
from pathlib import Path

# Add project root to path
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.models.data_prep import GameDataPrep

# ============================================================================
# UNDERSTANDING LOGISTIC REGRESSION
# ============================================================================
"""
What is Logistic Regression?
-----------------------------
Despite its name, it's a CLASSIFICATION algorithm, not regression!

The Math (simplified):
1. Takes your features: [margin_diff=7, home_ppg=28, ...]
2. Calculates a score: z = w₁*margin_diff + w₂*home_ppg + ... + b
3. Converts to probability: P(win) = 1 / (1 + e^(-z))
4. Makes decision: If P(win) > 0.5, predict home win

Why it's perfect for our baseline:
- LINEAR: Assumes features combine additively (7 point margin + 3 turnovers = X)
- INTERPRETABLE: Each coefficient tells us impact on win probability  
- FAST: Trains in seconds, not hours
- PROBABILISTIC: Gives confidence scores, not just yes/no

Limitations:
- Can't capture interactions (e.g., "high scoring + good defense = extra strong")
- Assumes linear relationships (2x the margin = 2x the impact)
- Single decision boundary (can't say "if margin > 10 OR turnovers > 3")
"""

class LogisticRegressionBaseline:
    """
    A interpretable baseline model for win prediction.
    Emphasizes understanding over pure performance.
    """

    def __init__(self, verbose=True, use_calibration=True):
        """Initialize with settings optimized for interpretability."""
        self.verbose = verbose
        self.use_calibration = use_calibration

        # Logistic Regression parameters explained:
        # - penalty='l2': Adds regularization to prevent overfitting
        # - C=1.0: Regularization strength (lower = stronger)
        # - max_iter=1000: Maximum iterations to converge
        # - random_state=42: Reproducibility
        self.base_model = LogisticRegression(
            penalty='l2',
            C=1.0,  # We'll tune this later
            max_iter=1000,
            random_state=42,
            verbose=0
        )

        self.model = None  # Will be set to base_model or calibrated version
        self.feature_names = None
        self.scaler = None
        
    def train(self, X_train, y_train, feature_names=None):
        """
        Train the logistic regression model with optional calibration.

        The training process:
        1. Initialize random weights
        2. For each iteration:
           - Calculate predictions
           - Measure error (log loss)
           - Adjust weights to reduce error
        3. Repeat until convergence
        4. (Optional) Apply probability calibration to fix overconfidence
        """
        if self.verbose:
            print("\n" + "="*60)
            print("TRAINING LOGISTIC REGRESSION" + (" (WITH CALIBRATION)" if self.use_calibration else ""))
            print("="*60)
            print(f"Training samples: {len(X_train)}")
            print(f"Features: {X_train.shape[1]}")
            print(f"Home win rate in training: {y_train.mean():.1%}")

        self.feature_names = feature_names

        if self.use_calibration:
            # Wrap base model with calibration
            # method='isotonic': Flexible calibration that can fix non-uniform miscalibration
            # cv=5: Use 5-fold CV to learn calibration mapping without overfitting
            if self.verbose:
                print("\nApplying isotonic calibration with 5-fold CV...")
                print("This will fix probability outputs so 80% confidence = 80% accuracy")

            self.model = CalibratedClassifierCV(
                self.base_model,
                method='isotonic',  # Handles non-linear calibration issues
                cv=5,               # 5-fold cross-validation for calibration
                ensemble=True       # Average predictions from all CV folds
            )
            self.model.fit(X_train, y_train)
        else:
            # Train without calibration
            self.model = self.base_model
            self.model.fit(X_train, y_train)

        # Get training accuracy
        train_pred = self.model.predict(X_train)
        train_acc = accuracy_score(y_train, train_pred)

        if self.verbose:
            print(f"\nTraining complete!")
            print(f"Training accuracy: {train_acc:.1%}")

            # Understand convergence (only available on base model)
            if hasattr(self.base_model, 'n_iter_') and not self.use_calibration:
                print(f"Converged in {self.base_model.n_iter_[0]} iterations")

        return self
    
    def evaluate(self, X_test, y_test, detailed=True):
        """
        Comprehensive evaluation with focus on understanding.
        """
        print("\n" + "="*60)
        print("MODEL EVALUATION")
        print("="*60)
        
        # Make predictions
        y_pred = self.model.predict(X_test) 
        y_prob = self.model.predict_proba(X_test)[:, 1]  #get confidence probability of prediction 
        
        # Basic metrics
        accuracy = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_prob)
        logloss = log_loss(y_test, y_prob)
        
        print(f"\n📊 Overall Performance:")
        print(f"  Accuracy: {accuracy:.1%}")
        print(f"  AUC-ROC: {auc:.3f}")
        print(f"  Log Loss: {logloss:.3f}")
        
        # Baseline comparison
        baseline_acc = max(y_test.mean(), 1 - y_test.mean())
        print(f"\n📈 Improvement over baseline:")
        print(f"  Baseline (always pick home/away): {baseline_acc:.1%}")
        print(f"  Our model: {accuracy:.1%}")
        print(f"  Improvement: +{(accuracy - baseline_acc):.1%}")
        
        if detailed:
            # Confusion Matrix - used to eval model results showing correct/incorrect numbers for each class of data
            cm = confusion_matrix(y_test, y_pred)
            print(f"\n📋 Confusion Matrix:")
            print(f"                 Predicted")
            print(f"                 Away  Home")
            print(f"  Actual Away  [{cm[0,0]:4d}, {cm[0,1]:4d}]")
            print(f"  Actual Home  [{cm[1,0]:4d}, {cm[1,1]:4d}]")
            
            # Interpretation
            print(f"\n💡 What this means:")
            print(f"  Correctly predicted {cm[0,0]} away wins")
            print(f"  Correctly predicted {cm[1,1]} home wins")
            print(f"  Incorrectly thought home would win {cm[0,1]} times")
            print(f"  Incorrectly thought away would win {cm[1,0]} times")
            
            # Classification report
            print(f"\n📊 Detailed Metrics:")
            print(classification_report(y_test, y_pred, 
                                       target_names=['Away Win', 'Home Win']))
            
            # Confidence analysis
            self._analyze_confidence(y_test, y_prob)
        
        return {
            'accuracy': accuracy,
            'auc': auc, 
            'log_loss': logloss,
            'predictions': y_pred,
            'probabilities': y_prob
        }
    
    def _analyze_confidence(self, y_true, y_prob):
        """Understand when the model is confident vs uncertain.
            Checking what the average correctness of a "70% confident prediction is" - are they actually right 70% of the time
        """
        
        print(f"\n🎯 Confidence Analysis:")
        
        # Bin predictions by confidence
        very_confident_home = y_prob > 0.7
        very_confident_away = y_prob < 0.3
        uncertain = (y_prob >= 0.4) & (y_prob <= 0.6)
        
        if very_confident_home.any():
            acc_confident_home = y_true[very_confident_home].mean()
            print(f"  When >70% confident in home: {acc_confident_home:.1%} accurate ({very_confident_home.sum()} games)")
        
        if very_confident_away.any():
            acc_confident_away = (1 - y_true[very_confident_away]).mean()
            print(f"  When >70% confident in away: {acc_confident_away:.1%} accurate ({very_confident_away.sum()} games)")
        
        if uncertain.any():
            acc_uncertain = ((y_prob[uncertain] > 0.5) == y_true[uncertain]).mean()
            print(f"  When uncertain (40-60%): {acc_uncertain:.1%} accurate ({uncertain.sum()} games)")
    
    def interpret_features(self, top_n=15):
        """
        Understand which features drive predictions.

        In logistic regression, coefficient magnitude = importance
        Positive coefficient = increases home win probability
        Negative coefficient = decreases home win probability

        """
        print("\n" + "="*60)
        print("FEATURE INTERPRETATION")
        print("="*60)

        if self.feature_names is None:
            print("No feature names provided!")
            return

        # Get coefficients from base model (calibration doesn't change feature importance)
        if self.use_calibration:
            # Access the base model's coefficients from the first calibrated classifier
            coefs = self.model.calibrated_classifiers_[0].estimator.coef_[0]
        else:
            coefs = self.model.coef_[0]
        
        # Create DataFrame for easy analysis
        feature_importance = pd.DataFrame({
            'feature': self.feature_names,
            'coefficient': coefs,
            'abs_coef': np.abs(coefs)
        }).sort_values('abs_coef', ascending=False)
        
        print(f"\n📈 Top {top_n} Most Important Features:")
        print("(Positive = favors home team, Negative = favors away team)\n")
        
        for idx, row in feature_importance.head(top_n).iterrows():
            direction = "→ HOME" if row['coefficient'] > 0 else "→ AWAY"
            print(f"  {row['feature']:30s}: {row['coefficient']:+.4f} {direction}")
        
        # Insight analysis
        self._analyze_feature_patterns(feature_importance)
        
        return feature_importance
    
    def _analyze_feature_patterns(self, feature_importance):
        """Identify patterns in what the model learned."""
        print("\n💡 Model Insights:")
        
        # Check if matchup features dominate
        matchup_features = feature_importance[
            feature_importance['feature'].str.contains('diff|vs', na=False)
        ]
        if len(matchup_features) > 0:
            matchup_importance = matchup_features['abs_coef'].sum()
            total_importance = feature_importance['abs_coef'].sum()
            print(f"  Matchup features: {matchup_importance/total_importance:.1%} of total importance")
        
        # Home vs Away features
        home_features = feature_importance[
            feature_importance['feature'].str.startswith('home_')
        ]
        away_features = feature_importance[
            feature_importance['feature'].str.startswith('away_')
        ]
        
        avg_home_coef = home_features['coefficient'].mean()
        avg_away_coef = away_features['coefficient'].mean()
        
        print(f"  Average home feature coefficient: {avg_home_coef:+.4f}")
        print(f"  Average away feature coefficient: {avg_away_coef:+.4f}")
        
        if avg_home_coef > abs(avg_away_coef):
            print(f"  → Model has learned home field advantage!")
    
    def visualize_results(self, X_test, y_test, save_path='logistic_results.png'):
        """Create comprehensive visualizations."""
        y_prob = self.model.predict_proba(X_test)[:, 1]
        y_pred = self.model.predict(X_test)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        
        # 1. ROC Curve - shows ability to seperate winners and losers 
        fpr, tpr, _ = roc_curve(y_test, y_prob)
        auc = roc_auc_score(y_test, y_prob)
        axes[0, 0].plot(fpr, tpr, label=f'ROC curve (AUC = {auc:.3f})')
        axes[0, 0].plot([0, 1], [0, 1], 'k--', label='Random')
        axes[0, 0].set_xlabel('False Positive Rate')
        axes[0, 0].set_ylabel('True Positive Rate')
        axes[0, 0].set_title('ROC Curve - How Well We Separate Classes')
        axes[0, 0].legend()
        
        # 2. Probability Distribution - shows how confident the model is in it's predictions
        axes[0, 1].hist(y_prob[y_test == 0], bins=20, alpha=0.5, label='Away Wins', color='blue')
        axes[0, 1].hist(y_prob[y_test == 1], bins=20, alpha=0.5, label='Home Wins', color='red')
        axes[0, 1].axvline(x=0.5, color='black', linestyle='--', label='Decision Boundary')
        axes[0, 1].set_xlabel('Predicted Probability of Home Win')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].set_title('Prediction Confidence Distribution')
        axes[0, 1].legend()
        
        # 3. Calibration Plot - shows if probabilities are accurate (70% cofidence is right 70% of the time)
        from sklearn.calibration import calibration_curve
        fraction_pos, mean_pred = calibration_curve(y_test, y_prob, n_bins=10)
        axes[1, 0].plot(mean_pred, fraction_pos, marker='o', label='Our Model')
        axes[1, 0].plot([0, 1], [0, 1], 'k--', label='Perfect Calibration')
        axes[1, 0].set_xlabel('Mean Predicted Probability')
        axes[1, 0].set_ylabel('Actual Win Fraction')
        axes[1, 0].set_title('Calibration - Do 70% Predictions Win 70% of Time?')
        axes[1, 0].legend()
        
        # 4. Feature Importance (Top 10) - ranks features
        if self.feature_names:
            # Get coefficients from base model
            if self.use_calibration:
                coefs = self.model.calibrated_classifiers_[0].estimator.coef_[0]
            else:
                coefs = self.model.coef_[0]

            feature_imp = pd.DataFrame({
                'feature': self.feature_names,
                'importance': np.abs(coefs)
            }).sort_values('importance', ascending=True).tail(10)
            
            axes[1, 1].barh(range(len(feature_imp)), feature_imp['importance'])
            axes[1, 1].set_yticks(range(len(feature_imp)))
            axes[1, 1].set_yticklabels(feature_imp['feature'])
            axes[1, 1].set_xlabel('Absolute Coefficient Value')
            axes[1, 1].set_title('Top 10 Most Important Features')
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=100, bbox_inches='tight')
        print(f"\n📊 Visualizations saved to {save_path}")
        plt.show()
        
        return fig
    
    def predict_game(self, home_features, away_features, feature_names):
        """
        Predict a single game with detailed explanation (which features contributed most to the prediction)
        Useful for understanding individual predictions
        """
        # Combine features as the model expects
        game_features = np.concatenate([home_features, away_features])
        
        # Get prediction and probability
        prob = self.model.predict_proba([game_features])[0, 1]
        prediction = "HOME" if prob > 0.5 else "AWAY"
        
        print(f"\n🏈 Game Prediction:")
        print(f"  Winner: {prediction}")
        print(f"  Confidence: {max(prob, 1-prob):.1%}")
        print(f"  Home Win Probability: {prob:.1%}")
        
        # Show top factors
        if self.feature_names:
            # Get coefficients from base model
            if self.use_calibration:
                coefs = self.model.calibrated_classifiers_[0].estimator.coef_[0]
            else:
                coefs = self.model.coef_[0]

            contributions = game_features * coefs
            top_factors_idx = np.argsort(np.abs(contributions))[-5:]
            
            print(f"\n  Top factors in this prediction:")
            for idx in top_factors_idx[::-1]:
                factor_name = self.feature_names[idx]
                factor_value = game_features[idx]
                factor_contribution = contributions[idx]
                direction = "→ HOME" if factor_contribution > 0 else "→ AWAY"
                print(f"    {factor_name}: {factor_value:.2f} ({factor_contribution:+.3f} {direction})")
        
        return prob

def main():
    """
    Complete training pipeline with educational focus.
    """
    print("\n" + "="*80)
    print("LOGISTIC REGRESSION BASELINE - D3 FOOTBALL PREDICTION")
    print("="*80)
    
    # 1. Load and prepare data
    print("\n📚 Step 1: Loading Data...")
    prep = GameDataPrep()
    data = prep.prepare_full_pipeline(
        start_year=2022,
        end_year=2024,
        min_week=4,
        target_type='home_win',
        test_year=2024,  # Train on 2022-2023, test on 2024
        normalize=True
    )
    
    print(f"✓ Data loaded and prepared")
    print(f"  Training games: {data['metadata']['n_train']}")
    print(f"  Test games: {data['metadata']['n_test']}")
    print(f"  Features: {data['metadata']['n_features']}")
    
    # 2. Train model
    print("\n📚 Step 2: Training Model...")
    model = LogisticRegressionBaseline(verbose=True)
    model.train(data['X_train'], data['y_train'], data['feature_names'])
    
    # 3. Evaluate performance
    print("\n📚 Step 3: Evaluating Performance...")
    results = model.evaluate(data['X_test'], data['y_test'])
    
    # 4. Interpret features
    print("\n📚 Step 4: Understanding What Model Learned...")
    feature_importance = model.interpret_features(top_n=15)
    
    # 5. Visualize results
    print("\n📚 Step 5: Creating Visualizations...")
    model.visualize_results(data['X_test'], data['y_test'])
    
    # 6. Save model (save the sklearn model, not the wrapper class)
    model_dir = Path('models')
    model_dir.mkdir(exist_ok=True)

    # Save just the trained sklearn model for prediction
    model_path = model_dir / 'logistic_regression.pkl'
    joblib.dump(model.model, model_path)  # Save model.model (the sklearn model)
    print(f"\n💾 Model saved to {model_path}")

    # Also save the wrapper for analysis
    wrapper_path = model_dir / 'logistic_baseline.pkl'
    joblib.dump(model, wrapper_path)
    print(f"💾 Full wrapper saved to {wrapper_path} (for analysis only)")
    
    # 7. Summary
    print("\n" + "="*80)
    print("BASELINE ESTABLISHED!")
    print("="*80)
    print(f"Final test accuracy: {results['accuracy']:.1%}")
    print(f"AUC-ROC: {results['auc']:.3f}")
    
    print("\n🎯 Next Steps:")
    print("  1. Try Random Forest (captures non-linear patterns)")
    print("  2. Try XGBoost (usually best for tabular data)")
    print("  3. Engineer more features based on what we learned")
    print("  4. Tune hyperparameters for better performance")
    
    return model, data, results

if __name__ == "__main__":
    model, data, results = main()