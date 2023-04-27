from environs import Env

class Config:
    def __init__(self):
        env = Env()
        self.host = env(
            "HOST",
            "http://localhost:9200", # fill in with environment variables
        )
        self.username = env("USERNAME", "") # fill in with environment variables
        self.password = env("PASSWORD", "") # fill in with environment variables


config = Config()
