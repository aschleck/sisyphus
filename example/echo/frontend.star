def main(ctx):
    return Application(
        args=[
            1,
            2,
            3,
            {
                "prod": 4,
                "dev": 5,
            }
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
