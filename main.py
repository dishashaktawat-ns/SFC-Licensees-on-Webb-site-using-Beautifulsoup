from config import Config
from src.orchestrator import SFCPipeline

if __name__ == "__main__":
    cfg = Config()

    pipeline = SFCPipeline(cfg)
    pipeline.run()