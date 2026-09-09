import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_worker() -> None:
    logger.info("Worker iniciado")

    while True:
        logger.info("Worker executando...")
        await asyncio.sleep(60)


def main() -> None:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("Worker encerrado")


if __name__ == "__main__":
    main()