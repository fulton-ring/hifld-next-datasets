import asyncio
import httpx
from pydantic import Field

def check_download_url(url: str) -> str:
    """Check if a URL is valid and seems to point to a downloadable file without downloading the entire file.
    
    Args:
        url: The URL to check
        
    Returns:
        A string describing the result, including HTTP status, content-type, and content-length if available.
    """
    try:
        # First try a HEAD request
        with httpx.Client(follow_redirects=True, timeout=10.0) as client:
            response = client.head(url)
            
            # Some servers reject HEAD requests with 405 Method Not Allowed or 403 Forbidden
            if response.status_code in (405, 403, 400, 501):
                # Fallback to GET with stream=True so we only fetch headers
                with client.stream("GET", url) as stream_response:
                    headers = stream_response.headers
                    status_code = stream_response.status_code
            else:
                headers = response.headers
                status_code = response.status_code
                
            content_type = headers.get("content-type", "unknown")
            content_length = headers.get("content-length", "unknown")
            
            if status_code >= 400:
                return f"URL returned error status: {status_code}"
                
            return f"Success (Status {status_code}). Content-Type: {content_type}, Content-Length: {content_length} bytes."
            
    except Exception as e:
        return f"Failed to check URL: {str(e)}"

if __name__ == "__main__":
    print(check_download_url("https://maritimeboundaries.noaa.gov/downloads/USMaritimeLimitsAndBoundariesSHP.zip"))
    print(check_download_url("https://maritimeboundaries.noaa.gov/fake-file.zip"))
