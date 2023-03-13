from fastapi import FastAPI
from fastapi.responses import FileResponse
import uvicorn
import os

module_dir = os.path.dirname(__file__)  # get current directory
data_folder = os.path.join(module_dir, 'data')

app = FastAPI()

@app.get("/")
def read_root():
    return {"msg": "Que onda Sofi"}

@app.get("/symbol_df/{symbol}", response_class=FileResponse)
def get_symbol_df(symbol):
    return FileResponse(os.path.join(data_folder, f"{symbol}.csv"))
