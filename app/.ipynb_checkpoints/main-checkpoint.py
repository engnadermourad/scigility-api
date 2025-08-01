from fastapi import FastAPI
import logging
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

from app.routes.parquet_report import router as parquet_report_router

from fastapi import Depends, HTTPException, status, Form
from fastapi.security import OAuth2PasswordRequestForm
from app.core.auth import authenticate_user, create_access_token

#________________________________________________________________________________________
# FastAPI app initialization with OpenAPI docs settings
app = FastAPI(
    title="live stream FastAPI", 
    version="1.0.0",
    docs_url="/docs",        # Path to Swagger UI docs
    openapi_url="/openapi.json"
)
# Enable CORS for frontend access from Streamlit
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://mx.streamlit.nmourmx.de"],  
    allow_credentials=True,                             
    allow_methods=["*"],
    allow_headers=["*"],
)
#_________________________________Routes __________________________________________________

# Include the router from the btc routes
app.include_router(parquet_report_router)


#________________________________Health check______________________________________________

# Health check route to verify the API is running
@app.get("/", response_model=dict)
def read_root():
    return {"message": "live stream API running."}
#____________________________________JWT___________________________________________________
@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    if not authenticate_user(form_data.username, form_data.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )
    token = create_access_token(data={"sub": form_data.username})
    return {"access_token": token, "token_type": "bearer"}

            

#_______________________fake login for pytest purpose______________________________________

# Temporary fake login endpoint for tests without real auth
@app.post("/login")
def fake_login():
    return {"access_token": "fake-jwt-token", "token_type": "bearer"}





