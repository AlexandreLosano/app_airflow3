# Airflow 3 — Ambiente institucional (estudo)

Stack Docker do **Apache Airflow 3.3.1** com **CeleryExecutor**, **PostgreSQL**
como banco de metadados e **Redis** como broker. Pensada como referência de
estudo para uso dentro de uma instituição: separa cada componente do Airflow
em seu próprio container, usa credenciais geradas (não fixas no código) e
aplica um conjunto básico de hardening.

## Arquitetura

```
                         ┌────────────────────┐
                         │   airflow-init      │  migração de schema +
                         │   (roda uma vez)     │  criação do usuário admin
                         └──────────┬───────────┘
                                    │
        ┌───────────────┬──────────┼───────────────┬────────────────┐
        │               │          │                │                │
 ┌──────▼─────┐  ┌───────▼──────┐ ┌▼─────────────┐ ┌▼─────────────┐ ┌▼─────────────┐
 │ apiserver  │  │  scheduler   │ │ dag-processor │ │  triggerer   │ │    worker    │
 │ (UI + API) │  │              │ │               │ │              │ │  (Celery)    │
 │ :8080      │  │              │ │               │ │              │ │              │
 └──────┬─────┘  └───────┬──────┘ └───────┬───────┘ └───────┬──────┘ └───────┬──────┘
        │                │                │                 │                │
        └────────────────┴────────────────┴─────────────────┴────────────────┘
                                    │                                   │
                          ┌─────────▼─────────┐              ┌─────────▼─────────┐
                          │     postgres        │              │       redis        │
                          │  (metadata DB)       │              │  (broker Celery)    │
                          │  sem porta no host    │              │  sem porta no host   │
                          └────────────────────┘              └────────────────────┘
```

Todos os componentes do Airflow leem a mesma imagem (`Dockerfile` local) e o
mesmo bloco de configuração (`x-airflow-common` no `docker-compose.yaml`),
evitando divergência de versão/dependências entre eles — um problema comum em
ambientes institucionais com múltiplos times mexendo na stack.

Componentes opcionais, ligados só sob demanda via *profile*:

- `flower` (`--profile flower`) — dashboard de monitoramento dos workers Celery.
- `airflow-cli` (`--profile debug`) — shell ad-hoc para rodar comandos `airflow ...`.

## Arquivos do projeto

| Arquivo/pasta        | Papel                                                                 |
|-----------------------|------------------------------------------------------------------------|
| `Dockerfile`           | Imagem customizada a partir de `apache/airflow:3.3.1` + `requirements.txt` |
| `docker-compose.yaml`  | Orquestra todos os serviços (Postgres, Redis, Airflow x5)              |
| `.env`                 | Credenciais **reais** deste ambiente — nunca vai para o git            |
| `.env.example`         | Template sem segredos, documenta como gerar cada valor                 |
| `requirements.txt`     | Providers/pacotes Python extras, fixados por versão                    |
| `dags/`                | Suas DAGs (montado por bind mount, editável sem rebuild de imagem)     |
| `plugins/`             | Plugins customizados do Airflow                                        |
| `config/`              | `airflow.cfg` e configs adicionais                                     |
| `logs/`                | Logs de execução (gerado em runtime, fora do git)                      |

## Como subir

```bash
# 1. Credenciais (pule se o .env já existir)
cp .env.example .env
#   - Gere POSTGRES_PASSWORD e _AIRFLOW_WWW_USER_PASSWORD: openssl rand -hex 20
#   - Gere AIRFLOW__CORE__FERNET_KEY:
#       python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
#   - Gere AIRFLOW__API_AUTH__JWT_SECRET e AIRFLOW__API__SECRET_KEY: openssl rand -hex 32
#   - Ajuste AIRFLOW_UID para `id -u` do seu usuário (permissões corretas em dags/logs/plugins)

# 2. Primeira inicialização: migra o banco e cria o usuário admin
docker compose up airflow-init

# 3. Sobe a stack completa
docker compose up -d

# 4. UI
open http://localhost:8080
# login: admin (ou o valor de _AIRFLOW_WWW_USER_USERNAME)
# senha: valor de _AIRFLOW_WWW_USER_PASSWORD no seu .env
```

Verificar saúde de tudo:

```bash
docker compose ps
curl -s http://localhost:8080/api/v2/monitor/health | jq
```

## Operações do dia a dia

```bash
# Logs de um serviço específico
docker compose logs -f airflow-scheduler

# Rodar comandos do Airflow CLI sem subir um shell interativo
docker compose run --rm airflow-cli airflow dags list

# Reiniciar só o scheduler depois de mudar uma config
docker compose restart airflow-scheduler

# Ligar o monitor de workers Celery (http://localhost:5555)
docker compose --profile flower up -d flower

# Parar tudo (mantém os dados do Postgres no volume nomeado)
docker compose down

# Parar tudo e apagar os dados do Postgres (⚠️ irreversível)
docker compose down -v
```

Depois de editar `requirements.txt` ou o `Dockerfile`, é preciso reconstruir a
imagem antes de subir de novo:

```bash
docker compose build
docker compose up -d
```

## Decisões de segurança já aplicadas

- **Postgres e Redis não expõem porta ao host** — só existem na rede interna
  `airflow-internal` do compose; nada além dos containers do Airflow consegue
  falar com eles diretamente.
- **Segredos vêm do `.env`, nunca do `docker-compose.yaml`** — o compose
  referencia `${VAR}`, e o `.env` real (com valores gerados aleatoriamente)
  está no `.gitignore`. Só o `.env.example`, sem segredos, é versionado.
- **`.env` com permissão `600`** — legível apenas pelo dono no host.
- **`AIRFLOW__API__EXPOSE_CONFIG: false`** — a configuração completa do
  Airflow (que pode conter segredos de connections, etc.) não fica acessível
  via UI/API.
- **`FabAuthManager`** habilitado em vez do auth manager simples de
  single-user — dá RBAC completo (múltiplos usuários, papéis, permissões
  granulares por DAG/conexão), essencial num ambiente institucional com mais
  de uma pessoa operando.
- **Fernet key própria** — todas as credenciais de *Connections* e
  *Variables* salvas no Postgres via UI ficam criptografadas com uma chave
  gerada só para este ambiente.
- **Container roda com o UID do host** (`AIRFLOW_UID`), não como root —
  reduz o impacto de uma eventual falha de isolamento do container e evita
  arquivos com dono `root` nos volumes montados.
- **`AIRFLOW__CORE__LOAD_EXAMPLES=false`** — sem DAGs de exemplo poluindo o
  ambiente institucional (fácil de ligar via `.env` para fins de estudo).

## Evoluindo para produção real

Este setup é adequado para estudo e para um ambiente de desenvolvimento/teste
institucional. Para produção de verdade, os próximos passos recomendados são:

1. **Gerenciador de segredos** — trocar o `.env` local por HashiCorp Vault,
   AWS Secrets Manager, Google Secret Manager ou similar. O Airflow suporta
   isso nativamente via o backend de secrets (`[secrets] backend =`
   `airflow.providers.hashicorp.secrets.vault.VaultBackend`, por exemplo),
   assim nem a *Fernet key* nem as senhas do Postgres ficam em disco.
2. **TLS/HTTPS** — hoje a UI (`:8080`) responde em HTTP puro. Em produção,
   colocar um reverse proxy (nginx, Traefik) ou load balancer na frente com
   certificado válido, e não expor a porta 8080 diretamente.
3. **Postgres gerenciado** — usar um serviço gerenciado (RDS, Cloud SQL) com
   backup automático, em vez do container `postgres:16` com volume local.
4. **Rotação de segredos** — Fernet key, JWT secret e secret key da API devem
   ter um processo de rotação periódica (a Fernet key suporta múltiplas
   chaves simultâneas para permitir rotação sem downtime).
5. **Autenticação corporativa** — integrar o `FabAuthManager` com LDAP/OIDC/SSO
   da instituição em vez de usuários locais.
6. **Observabilidade** — exportar métricas (`AIRFLOW__METRICS__STATSD_ON`) e
   logs para uma stack central (Prometheus/Grafana, ELK, etc.) em vez de
   depender só do `docker compose logs`.
7. **Orquestração além de um único host** — em escala institucional real, o
   caminho natural é migrar de `docker compose` para Kubernetes com o
   `KubernetesExecutor` (ou o Helm chart oficial do Airflow), que isola cada
   *task* em seu próprio pod.

## Solução de problemas

- **`docker compose up` falha com erro de permissão em `dags/`/`logs/`** —
  confira se `AIRFLOW_UID` no `.env` bate com `id -u` do seu usuário no host.
- **Login falha na UI** — confirme o usuário/senha em `_AIRFLOW_WWW_USER_*`
  no `.env`; eles só são aplicados na *primeira* execução do `airflow-init`.
  Para trocar a senha depois, use a UI (Security → List Users) ou
  `docker compose run --rm airflow-cli airflow users reset-password ...`.
- **Mudou uma variável em `.env` e nada mudou** — variáveis de `env_file` só
  são recarregadas quando o container é recriado: `docker compose up -d --force-recreate`.
