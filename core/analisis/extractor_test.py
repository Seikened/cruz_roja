import polars as pl
from pathlib import Path
from colorstreak import Logger as log

DATA_DIR = Path(__file__).resolve().parent / "data"
file = DATA_DIR / "dataset_feb_cruz_roja.csv"

log.note(f"Ruta:{DATA_DIR} | Existe?:{file.exists()}")


lf = (
    pl.scan_csv(file)
    .drop_nulls(subset=["CECOM"])
    .drop(["OBSERVACIONES",])
    .select(
        pl.all()
        .name.to_lowercase()
        .name.replace(" ", "_")
    )
)

df = lf.collect()


log.debug(df.columns)
