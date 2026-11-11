"""Modelo supervisado de impago con variables financieras y calibración independiente."""
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV, calibration_curve
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import StratifiedGroupKFold
from xgboost import XGBClassifier

MODEL_VERSION = 'credit-default-xgb-calibrated-v1'


def build_credit_features(raw):
    required = ['X1', *[f'X{i}' for i in range(6,24)]]
    missing = set(required)-set(raw.columns)
    if missing:
        raise ValueError(f'Faltan variables financieras {sorted(missing)}')
    source = raw[required].apply(pd.to_numeric,errors='raise').astype(float)
    if not np.isfinite(source.to_numpy()).all() or (source.X1<=0).any():
        raise ValueError('Las variables deben ser finitas y el límite positivo')
    features = pd.DataFrame(index=raw.index)
    for month in range(1,7):
        bill = source[f'X{month+11}']
        payment = source[f'X{month+17}']
        delay = source[f'X{month+5}']
        if (payment<0).any() or (~delay.between(-2,9)).any():
            raise ValueError('Pagos o estados de atraso fuera del contrato')
        features[f'utilization_{month}'] = (bill/source.X1).clip(-1,5)
        features[f'payment_share_{month}'] = np.where(bill>0,(payment/bill).clip(0,5),0.)
        features[f'delay_{month}'] = delay.clip(lower=0)
    features['max_delay'] = features[[f'delay_{i}' for i in range(1,7)]].max(axis=1)
    features['late_months'] = (features[[f'delay_{i}' for i in range(1,7)]]>0).sum(axis=1)
    return features


def train_model(raw, seed=42):
    features = build_credit_features(raw)
    target = pd.to_numeric(raw['Y'],errors='raise').astype(int)
    if not target.isin([0,1]).all() or target.nunique()!=2:
        raise ValueError('La etiqueta de impago debe contener 0 y 1')
    # Los registros con el mismo vector financiero permanecen en el mismo grupo.
    groups = pd.util.hash_pandas_object(features,index=False).to_numpy()
    folds = np.empty(len(features),dtype=int)
    splitter = StratifiedGroupKFold(n_splits=5,shuffle=True,random_state=seed)
    for fold,(_,indices) in enumerate(splitter.split(features,target,groups)):
        folds[indices] = fold
    train,calibration,test = folds>=2,folds==1,folds==0
    estimator = XGBClassifier(n_estimators=250,max_depth=3,learning_rate=.06,
                              subsample=.85,colsample_bytree=.85,reg_lambda=2.,
                              random_state=seed,n_jobs=2,eval_metric='logloss',tree_method='hist')
    estimator.fit(features[train],target[train])
    model = CalibratedClassifierCV(estimator,method='sigmoid',cv='prefit')
    model.fit(features[calibration],target[calibration])
    probability = model.predict_proba(features[test])[:,1]
    baseline = np.full(test.sum(),target[train].mean())
    observed,predicted = calibration_curve(target[test],probability,n_bins=8,strategy='quantile')
    metrics = {'model_version':MODEL_VERSION,'seed':seed,'rows':len(raw),
               'train_rows':int(train.sum()),'calibration_rows':int(calibration.sum()),'test_rows':int(test.sum()),
               'roc_auc':float(roc_auc_score(target[test],probability)),
               'average_precision':float(average_precision_score(target[test],probability)),
               'brier_score':float(brier_score_loss(target[test],probability)),
               'baseline_brier':float(brier_score_loss(target[test],baseline)),
               'log_loss':float(log_loss(target[test],probability)),
               'default_rate_test':float(target[test].mean()),'feature_names':list(features.columns),
               'calibration':[{'predicted':float(p),'observed':float(o)} for p,o in zip(predicted,observed)],
               'split_method':'StratifiedGroupKFold: 60% entrenamiento, 20% calibración, 20% prueba',
               'protected_variables_used':False,'target':'Impago del mes siguiente, etiqueta UCI Y',
               'limitations':'Cohorte pública de Taiwán de 2005. La aplicación a la persona ficticia es orientativa y no está validada como decisión bancaria.'}
    return {'model':model,'feature_names':list(features.columns),'version':MODEL_VERSION},metrics


def score_profile(bundle, cards):
    recent = cards.sort_values('period_month',ascending=False).head(6)
    if len(recent)!=6:
        raise ValueError('Se requieren seis meses de historia de tarjeta')
    record = {'X1':float(recent.iloc[0].credit_limit)}
    for index,row in enumerate(recent.itertuples(),start=1):
        record[f'X{index+5}']=float(row.delay_months)
        record[f'X{index+11}']=float(row.balance)
        record[f'X{index+17}']=float(row.payments)
    features=build_credit_features(pd.DataFrame([record]))[bundle['feature_names']]
    probability=float(bundle['model'].predict_proba(features)[0,1])
    level='Bajo' if probability<.15 else 'Moderado' if probability<.30 else 'Elevado'
    return {'probability':probability,'level':level,'model_version':bundle['version'],
            'features':features.iloc[0].to_dict(),'use':'Orientación académica sobre un perfil ficticio'}
