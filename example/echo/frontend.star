def main(ctx):
    return Application(
        args=[
            "--service_spec",
            {
                "prod": "backend-prod.svc",
                "test": "backend-test.svc",
                "dev": "backend-test.svc",
            },
        ],
        env={
            "COLOR": "pink",
            "SECRET_TOKEN": StringVariable("secret-token"),
            "GOOGLE_APPLICATION_CREDENTIALS": (
                FileVariable(name="google-credentials", path="/etc/google/credentials.json")
            ),
            "HTTP_PORT": {
                "prod": Port(name="http", number=80),
                "dev": Port(name="http", number=8080),
            },
        },
    )
