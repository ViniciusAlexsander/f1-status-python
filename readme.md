# F1 Status API

API REST em FastAPI para consultar eventos de Formula 1 usando a OC Blacktop API.
Tambem expoe streams SSE de live timing usando uma conexao SignalRCore unica
com o live timing oficial da Formula 1.

## Rodando localmente

Crie um arquivo `.env` na raiz:

```env
OCBLACKTOP_API_KEY=sua_chave_aqui
OCBLACKTOP_API_BASE_URL=https://api.ocblacktop.com/v1
CORS_ALLOW_ORIGINS=http://localhost:5173
FORMULA1_SUBSCRIPTION_TOKEN=seu_subscription_token_aqui
PORT=8000
```

Se `FORMULA1_SUBSCRIPTION_TOKEN` nao for definido, a API tenta ler o token de
um arquivo local de dados do usuario. Para usar os streams ao vivo, o token
precisa ser valido.

Com Docker Compose, esse `.env` tambem e carregado pelo servico `api`.
Em deploys fora do Compose, configure `OCBLACKTOP_API_KEY` nas variaveis de
ambiente da plataforma; definir apenas `PORT` nao e suficiente para iniciar a
API.

## Live timing

Endpoints SSE:

- `GET /api/v1/live-timing/timing`
- `GET /api/v1/live-timing/session`
- `GET /api/v1/live-timing/current-tyres`
- `GET /api/v1/live-timing/session-info`
- `GET /api/v1/live-timing/extrapolated-clock`
- `GET /api/v1/live-timing/weather-data`
- `GET /api/v1/live-timing/race-control-messages`

Os streams de `SessionInfo`, `ExtrapolatedClock`, `WeatherData` e
`RaceControlMessages` transmitem o payload JSON recebido sem transformação ou
persistência no Redis. Esses tópicos são incluídos automaticamente mesmo quando
`LIVETIMING_SIGNALR_TOPICS` é configurado manualmente.
