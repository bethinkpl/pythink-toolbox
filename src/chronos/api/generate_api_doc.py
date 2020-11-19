import json

from chronos.api.main import app


def _get_swagger_ui_html(
    *,
    spec_url: str,
    title: str,
    swagger_js_url: str = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@3.30.0/swagger-ui-bundle.js",
    swagger_css_url: str = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@3.30.0/swagger-ui.css",
    swagger_favicon_url: str = "https://fastapi.tiangolo.com/img/favicon.png",
) -> str:
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
        spec: spec,"""

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

    html += """
    </script>
    </body>
    </html>
    """.strip()
    return html


with open("docs/spec.js", "w") as file:
    file.write(f"var spec = {json.dumps(app.openapi()).strip()}\n")

with open("docs/index.html", "w") as file:
    index = _get_swagger_ui_html(
        spec_url="./spec.js",
        title=app.title + " - Swagger UI",
    )
    file.write(f"{index}\n")
