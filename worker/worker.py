import asyncio
import logging

from api.core.config import get_settings
from api.domain.ocblacktop_client import OcblacktopClient
from api.services.race_service import RaceService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_worker() -> None:
    logger.info("Worker iniciado")
    settings = get_settings()

    ocblacktop_client = OcblacktopClient(
        base_url=settings.ocblacktop_api_base_url,
        api_key=settings.ocblacktop_api_key,
    )

    race_service = RaceService(
        client=ocblacktop_client,
    )

    while True:
        logger.info("Worker executando...")

        try:
            races = await race_service.list_races()

            if races.data.currentRace:
                logger.info(f"Corrida em andamento: {races.data.currentRace.name}, no dia {races.data.currentRace.dateStart}")
            elif races.data.nextRace:
                logger.info(f"Próxima corrida: {races.data.nextRace.name}, no dia {races.data.nextRace.dateStart}")
            else:
                logger.info("Nenhuma corrida em andamento ou próxima encontrada")
        except Exception as e:
            logger.error("Erro ao buscar corridas: %s", e)

        await asyncio.sleep(60)


def main() -> None:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("Worker encerrado")


if __name__ == "__main__":
    main()