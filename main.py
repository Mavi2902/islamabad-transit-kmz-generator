from fastapi import FastAPI, Response, Header, Depends, HTTPException, status
from pydantic import BaseModel, HttpUrl
from typing import Annotated
import asyncio
import os
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env

from generator import generate_kmz_file
 
app = FastAPI()


class KmzRequest(BaseModel):
    gtfs_url: HttpUrl = os.getenv("GTFS_URL", None)


@app.post("/generate-kmz")
async def create_kmz(
    request: KmzRequest,
    output_format: str = "kmz",
    github_token: Annotated[str | None, Header()] = None,
):
    """
    Generates a KMZ file from a GTFS feed URL and a GitHub token for private metro line data.
    """
    # Use environment variable as default if not provided in header
    final_github_token = github_token or os.getenv("GITHUB_TOKEN")
    if not final_github_token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="GITHUB_TOKEN not provided in header or .env file",
        )

    # Use environment variable as default if not provided in request body
    final_gtfs_url = request.gtfs_url
    if not final_gtfs_url:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="GTFS_URL not provided in request body or .env file",
        )
    
    if output_format not in ["kmz", "kml"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid output_format. Must be 'kmz' or 'kml'",
        )

    # Running the synchronous file generation in a thread pool
    data = await generate_kmz_file(
        gtfs_url=str(final_gtfs_url),
        github_token=final_github_token,
        output_format=output_format,
    )

    if output_format == "kml":
        media_type = "application/vnd.google-earth.kml+xml"
        filename = "Islamabad_Transit.kml"
    else:
        media_type = "application/vnd.google-earth.kmz"
        filename = "Islamabad_Transit.kmz"

    return Response(
        content=data,
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
