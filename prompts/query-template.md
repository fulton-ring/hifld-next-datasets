Inventory metadata:
{metadata}

Please search using the query: {query}
For every geospatial file you find (e.g., shapefile, GeoJSON, KMZ, GDB), report the file name/description, format, and one or more downloadable URLs that appear on the source agency’s domain. Target US government agencies and avoid data.gov mirrors or other caches when possible. Use DuckDuckGo to discover candidate pages and call the Crawl4aiTools when you need page snippets or confirmations. Use the `check_download_url` tool to verify that the final URLs actually point to downloadable files. The Crawl4aiTools are especially important for scraping JavaScript-heavy sites (e.g., sites that require rendering to reveal hidden links) or extracting links hidden within PDFs. The data rescue project portal (https://portal.datarescueproject.org/datasets/) is a helpful starting point when tracking down official agencies or source URLs.
Return JSON that validates against the DatasetSources schema described below:
  * dataset (string)
  * publisher (string, optional)
  * summary (string, optional)
  * query (string)
  * files (list of DatasetFile with keys: name, format, urls)
  * candidates (list of SourceCandidate with keys: label, url, confidence, notes)
Only include well-justified, government-source URLs and score them between 0 and 1; mention the format for each file’s link. Do not invent data; if you cannot find a credible candidate, emit an empty list.
