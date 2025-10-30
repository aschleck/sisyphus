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
                "test": Port(name="http", number=80),
                "dev": Port(name="http", number=8080, protocol="TCP"),  # protocol defaults to TCP
            },
        },
        resources=Resources(
            requests={
                "cpu": {
                    "prod": 16,
                    "test": 2,
                    "dev": None,
                },
                "memory": {
                    "prod": "8Gb",
                    "test": "2Gb",
                },
            },
            limits={
                "cpu": 32,
                "memory": "16Gb",
            },
        ),
    )
