import json
from typing import Optional

from chronos.api.main import app
from fastapi.encoders import jsonable_encoder
from starlette.responses import HTMLResponse


def get_swagger_ui_html(
    *,
    spec_url: str,
    title: str,
    swagger_js_url: str = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@3.30.0/swagger-ui-bundle.js",
    swagger_css_url: str = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@3.30.0/swagger-ui.css",
    swagger_favicon_url: str = "https://fastapi.tiangolo.com/img/favicon.png",
    oauth2_redirect_url: Optional[str] = None,
    init_oauth: Optional[dict] = None,
) -> HTMLResponse:
    """
    Get swagger documentation from json.
    """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <link type="text/css" rel="stylesheet" href="{swagger_css_url}">
    <link rel="shortcut icon" href="{swagger_favicon_url}">
    <title>{title}</title>
    </head>
    <body>
    <div id="swagger-ui">
    </div>
    <script src="{swagger_js_url}"></script>
    <script src="{spec_url}"></script>
    <!-- `SwaggerUIBundle` is now available on the page -->
    <script>

    const ui = SwaggerUIBundle({{
        spec: spec,
    """

    if oauth2_redirect_url:
        html += f"oauth2RedirectUrl: window.location.origin + '{oauth2_redirect_url}',"

    html += """
        dom_id: '#swagger-ui',
        presets: [
        SwaggerUIBundle.presets.apis,
        SwaggerUIBundle.SwaggerUIStandalonePreset
        ],
        layout: "BaseLayout",
        deepLinking: true,
        showExtensions: true,
        showCommonExtensions: true
    })"""

    if init_oauth:
        html += f"""
        ui.initOAuth({json.dumps(jsonable_encoder(init_oauth))})
        """

    html += """
    </script>
    </body>
    </html>
    """.strip()
    return html


with open("docs/spec.js", "w") as file:
    """
    Save API documentation in json format.
    """
    file.write(f"var spec = {json.dumps(app.openapi()).strip()}\n")

with open("docs/index.html", "w") as file:
    """
    Save API documentation in html format.
    """
    index = get_swagger_ui_html(
        spec_url="./spec.js",
        title=app.title + " - Swagger UI",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
    )
    file.write(f"{index}\n")
