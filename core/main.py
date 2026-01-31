from fastapi import FastAPI



app = FastAPI(
    title="My API",
    description="This is a sample API built with FastAPI.",
    version="1.0.0"
)



@app.get("/")
async def read_root():
    return {"message": "Welcome to My API!"}