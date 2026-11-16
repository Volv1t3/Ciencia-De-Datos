"""Red neuronal (MLP en PyTorch) con interfaz de scikit-learn (fit / predict_proba / get_params).

Entrenamiento propio en vez de skorch: los datos se suben a la GPU UNA vez y cada lote se toma por
índice directamente ahí. skorch arma los lotes fila por fila con un DataLoader, y con decenas de
millones de filas eso dominaba el tiempo (horas por pliegue con la GPU casi ociosa).

Vive en su propio módulo, a nivel de módulo, para que pickle/cloudpickle lo encuentren por nombre al
guardar y cargar el Pipeline desde MLflow. El modelo entrenado se guarda en CPU (portable).
"""

from __future__ import annotations

import numpy as np
import torch
from sklearn.base import BaseEstimator, ClassifierMixin


class RedMLP(torch.nn.Module):
    """Capas densas con ReLU y dropout."""

    def __init__(self, n_entrada: int, ocultas=(256, 128), dropout: float = 0.1):
        super().__init__()
        capas, previo = [], n_entrada
        for h in ocultas:
            capas += [torch.nn.Linear(previo, h), torch.nn.ReLU(), torch.nn.Dropout(dropout)]
            previo = h
        capas.append(torch.nn.Linear(previo, 1))
        self.red = torch.nn.Sequential(*capas)

    def forward(self, X):
        return self.red(X).squeeze(-1)


class MLPBinaria(BaseEstimator, ClassifierMixin):
    """MLP para clasificación binaria con BCE ponderada (pos_weight) y AdamW."""

    def __init__(self, ocultas=(256, 128), dropout: float = 0.1, lr: float = 2e-3,
                 weight_decay: float = 1e-5, max_epochs: int = 6, batch_size: int = 32768,
                 pos_weight: float = 50.0, random_state: int = 42, device: str | None = None):
        self.ocultas = ocultas
        self.dropout = dropout
        self.lr = lr
        self.weight_decay = weight_decay
        self.max_epochs = max_epochs
        self.batch_size = batch_size
        self.pos_weight = pos_weight
        self.random_state = random_state
        self.device = device

    def _dispositivo(self):
        return torch.device(self.device or ("cuda" if torch.cuda.is_available() else "cpu"))

    def fit(self, X, y):
        dev = self._dispositivo()
        torch.manual_seed(self.random_state)
        Xt = torch.as_tensor(np.ascontiguousarray(X, dtype=np.float32), device=dev)
        yt = torch.as_tensor(np.asarray(y, dtype=np.float32), device=dev)
        red = RedMLP(Xt.shape[1], tuple(self.ocultas), self.dropout).to(dev)
        opt = torch.optim.AdamW(red.parameters(), lr=self.lr, weight_decay=self.weight_decay)
        perdida = torch.nn.BCEWithLogitsLoss(pos_weight=torch.tensor(float(self.pos_weight), device=dev))
        gen = torch.Generator(device=dev).manual_seed(self.random_state)
        red.train()
        self.perdida_por_epoca_ = []
        for _ in range(self.max_epochs):
            orden = torch.randperm(len(Xt), device=dev, generator=gen)
            total = 0.0
            for i in range(0, len(Xt), self.batch_size):
                idx = orden[i:i + self.batch_size]
                opt.zero_grad(set_to_none=True)
                loss = perdida(red(Xt[idx]), yt[idx])
                loss.backward()
                opt.step()
                total += loss.item() * len(idx)
            self.perdida_por_epoca_.append(total / len(Xt))
        self.red_ = red.to("cpu").eval()
        self.n_features_in_ = Xt.shape[1]
        self.classes_ = np.array([0, 1])
        del Xt, yt
        if dev.type == "cuda":
            torch.cuda.empty_cache()
        return self

    @torch.no_grad()
    def predict_proba(self, X, lote: int = 2_000_000):
        dev = self._dispositivo()
        red = self.red_.to(dev)
        partes = []
        for i in range(0, len(X), lote):  # por bloques: 10M filas × 480 columnas de una vez no caben
            Xt = torch.as_tensor(np.ascontiguousarray(X[i:i + lote], dtype=np.float32), device=dev)
            partes.append(torch.sigmoid(red(Xt)).float().cpu().numpy())
            del Xt
        self.red_ = red.to("cpu")
        p = np.concatenate(partes) if partes else np.zeros(0, dtype=np.float32)
        return np.column_stack([1 - p, p])

    def predict(self, X):
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)


def construir_mlp(params: dict, seed: int) -> MLPBinaria:
    p = dict(params)
    if "ocultas" in p:
        p["ocultas"] = tuple(p["ocultas"])
    return MLPBinaria(random_state=seed, **p)
