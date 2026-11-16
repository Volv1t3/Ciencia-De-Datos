"""Red neuronal (MLP en PyTorch) con interfaz de scikit-learn vía skorch.

Vive en su propio módulo, a nivel de módulo, para que pickle/cloudpickle encuentren las clases
por nombre al guardar y cargar el Pipeline desde MLflow. Importa torch: solo se carga cuando se
construye la familia "mlp".
"""

from __future__ import annotations

import numpy as np
import torch
from skorch import NeuralNetBinaryClassifier


class RedMLP(torch.nn.Module):
    """Capas densas con ReLU y dropout; la primera es LazyLinear (toma el número de features)."""

    def __init__(self, ocultas=(256, 128), dropout: float = 0.1):
        super().__init__()
        capas, previo = [], None
        for h in ocultas:
            capas += [torch.nn.LazyLinear(h) if previo is None else torch.nn.Linear(previo, h),
                      torch.nn.ReLU(), torch.nn.Dropout(dropout)]
            previo = h
        capas.append(torch.nn.Linear(previo, 1))
        self.red = torch.nn.Sequential(*capas)

    def forward(self, X):
        return self.red(X).squeeze(-1)


class MLPBinaria(NeuralNetBinaryClassifier):
    """skorch espera X float32 y la etiqueta como float (BCEWithLogitsLoss)."""

    def fit(self, X, y=None, **fit_params):
        return super().fit(np.asarray(X, dtype=np.float32), np.asarray(y, dtype=np.float32), **fit_params)

    def predict_proba(self, X):
        return super().predict_proba(np.asarray(X, dtype=np.float32))


def construir_mlp(params: dict, seed: int) -> MLPBinaria:
    torch.manual_seed(seed)
    # Lotes grandes: con 8.192 filas por lote la GPU esperaba a la CPU (40 min por pliegue con
    # 10 épocas). Con 32.768 hay 4× menos lotes; lr algo mayor para compensar.
    p = {"ocultas": (256, 128), "dropout": 0.1, "lr": 2e-3, "weight_decay": 1e-5,
         "max_epochs": 6, "batch_size": 32768, "pos_weight": 50.0} | params
    return MLPBinaria(
        RedMLP, module__ocultas=tuple(p["ocultas"]), module__dropout=p["dropout"],
        criterion=torch.nn.BCEWithLogitsLoss,
        criterion__pos_weight=torch.tensor(p["pos_weight"]),
        optimizer=torch.optim.AdamW, optimizer__weight_decay=p["weight_decay"],
        lr=p["lr"], max_epochs=p["max_epochs"], batch_size=p["batch_size"],
        iterator_train__shuffle=True, train_split=None, verbose=0,
        device="cuda" if torch.cuda.is_available() else "cpu",
    )
