"""
main.py - The entry point for our FastAPI application

This file does three things:
1. Creates the FastAPI application instance
2. Configures CORS (so browsers allow our frontend to talk to this API)
3. Defines routes (URLs that return data)
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

#create the app instance, this is what will be run
app = FastAPI(
    title = "D3 Football Predictions API ",
    description = "API for D3 college football game predictions and statistics",
    version = "1.0.0"
)

#configure CORS, prevents frontend from blocking access due to different ports/origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],    #any website can call the api
    allow_credentials=True, #allows cookies and auth headers to be sent 
    allow_methods=["*"],     #allows all HTTP methods like GET and POST
    allow_headers=["*"]      #allow all headers 
)   

@app.get("/") # when someone makes a get request to "/", run this and return this message
def root():
    """
    Root endpoint just to confirm that the API is running
    Visit http://localhost:8000/ to see this response
    """ 
    return {
        "message" : "D3 Football Predictions API",
        "status"  : "RUNNING",
        "docs" : "Visit /docs for interactive API documentation"
    }

@app.get("/api/health")
def health_check():
    """
    Health Check Endpoint
    Deployment platforms will use to check if service is alive
    """
    return{
        "status": "healthy"
    }

