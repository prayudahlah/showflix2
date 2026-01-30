# showflix2

## setup

#### setup environment
```bash
cp .env.example .env
```

fill the missing variables such as passwords and token. Use `openssl rand -hex 32` for `GARAGE_RPC_SECRET`

#### run docker
```bash
docker-compose up --build
```
