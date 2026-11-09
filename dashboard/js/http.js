// Body-less GET requests remain simple CORS requests (F5).
export function requestHeaders(options = {}) {
  const headers = {...options.headers};
  if (options.body !== undefined && options.body !== null) {
    if (!Object.keys(headers).some((name) => name.toLowerCase() === "content-type")) {
      headers["Content-Type"] = "application/json";
    }
  }
  return headers;
}
