# Sistema de recomendación de juegos

Este repositorio contiene un conjunto de **recomendadores de juegos** basados en:
- Popularidad global (`top_n`)
- Co-ocurrencia de juegos (`pares_de_juegos`)
- Perfiles de usuario (género / distribuidor)
- Modelos de ML previamente entrenados (SVD, Dos Torres, Gran Torre)

La idea principal es poder **recomendar juegos a un usuario** a partir de sus interacciones históricas almacenadas en una base SQLite.

---

## Estructura mínima de directorios

```text
.
├── recomendar.py            # Módulo principal de recomendación (este archivo)
├── metricas.py              # Funciones de evaluación (ej. NDCG)
└── datos/                   # Datos y modelos entrenados
    ├── metacritics_bk.db    # Base SQLite con usuarios, juegos, interacciones
    ├── SVD.pkl              # (opcional) modelo SVD de Surprise (no siempre usado)
    ├── svd_params.npz       # Parámetros del SVD exportado (usado por predict_svd)
    ├── dt_checkpoint.0.10.keras   # (opcional) modelo "Dos Torres"
    ├── mappings_dt.pkl            # (opcional) mappings para Dos Torres
    ├── gt_checkpoint.0.11.keras   # (opcional) modelo "Gran Torre"
    └── mappings.pkl              # (opcional) mappings para Gran Torre
