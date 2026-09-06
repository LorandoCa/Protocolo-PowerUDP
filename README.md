Documentação abstrata do protocolo **powerUDP**, um protocolo de camada de transporte fiável desenvolvido em linguagem **C** sobre a API de Sockets POSIX. O projeto contempla um servidor central de controlo (TCP/Multicast), clientes concorrentes (*multithreaded*). Para casos teste, terá sido feita uma simulação de topologia de rede no **GNS3** e a implementação do protocolo nos integrantes da rede. 

---

## 📋 Índice

- [Visão Geral do Sistema](#visao-geral-do-sistema)
- [Funcionalidades do PowerUDP](#funcionalidades-do-powerUDP)
- [Estruturas de Dados e Mapeamento do Protocolo](#estruturas-de-dados-e-mapeamento-do-protocolo)
- [Arquitetura do Servidor](#arquitetura-do-servidor)
- [Arquitetura do Cliente](#arquitetura-do-cliente)
- [Protocolo powerUDP & Algoritmos de Fiabilidade](#protocolo-powerUDP-&-algoritmos-de-fiabilidade)
- [Cenário de Rede e NAT em GNS3](#Cenário-de-Rede-e-NAT-em-GNS3)
- [Autores](#autores)

---

## Visão Geral do Sistema

O **powerUDP** resolve o problema da falta de fiabilidade intrínseca do protocolo UDP (*User Datagram Protocol*). A arquitetura divide-se em duas camadas operacionais:

1. **Plano de Controlo (TCP + Multicast):**
   * **TCP (Porto 443):** Estabelecido entre o cliente e o servidor central para autenticação inicial via *Pre-Shared Key* (PSK) e atualização pontual de parâmetros de rede.
   * **UDP Multicast (Grupo `239.0.0.1:12345`):** Utilizado pelo servidor para difundir (*broadcast*) alterações globais de configuração a todos os clientes ativos em tempo real.
2. **Plano de Dados (UDP Peer-to-Peer / powerUDP):**
   * **UDP Unicast (Porto 444 / Personalizado):** Os clientes comunicam diretamente entre si, transmitindo frases fragmentadas palavra a palavra com controlo de erros, janelas de retratamento, números de sequência e ACKs.

```text
                                   +-------------------+
                                   |  Servidor Central |
                                   |  (10.20.0.129)    |
                                   +---------+---------+
                                             |
                   [1. Registo TCP / PSK]    |  [2. Multicast Config]
                   (Porto 443)               |  (239.0.0.1:12345)
                                             v
     +---------------------------------------+---------------------------------------+
     |                                                                               |
     v                                                                               v
+------------+                                                                 +------------+
|  Cliente 1 | <================== [3. powerUDP Data Link] ==================> |  Cliente 2 |
| (Porto 444)|                     (UDP Unicast + ACKs)                        |(Porto 444) |
+------------+                                                                 +------------+

```
--- 

## Funcionalidades do powerUDP

* **Controlo de Sequência:** Identificação numérica de pacotes para ordenação e rejeição de duplicados.
* **Mecanismo de Retransmissão (ARQ):** Reenvio automático de palavras perdidas até atingir o limite `max_retries`.
* **Exponential Backoff:** Aumento exponencial do tempo de *timeout* a cada tentativa falhada de reenvio.
* **Simulação de Erros:** Injeção configurável de perda de pacotes (0% a 100%) para testes de resiliência.
* **Concorrência Segura:** Utilização de `pthreads` e `mutexes` no servidor para gerir múltiplos clientes e acessos concorrentes sem *race conditions*.

---

## Estruturas de Dados e Mapeamento do Protocolo

Tanto o cliente como o servidor partilham definições binárias rigorosas para a troca de mensagens na rede:

### 1. Configuração do Protocolo (CONFIGMESSAGE)

Define o comportamento dos algoritmos de fiabilidade:

```c
typedef struct ConfigMessage {
    uint8_t enable_retransmission;  // 0 = Desativado, 1 = Ativado
    uint8_t enable_backoff;         // 0 = Desativado, 1 = Ativado (Exponential Backoff)
    uint8_t enable_sequence;        // 0 = Desativado, 1 = Ativado
    uint16_t base_timeout;          // Tempo base para timeout em milissegundos (ms)
    uint8_t max_retries;            // Número máximo de retransmissões permitidas
} CONFIGMESSAGE;
```

### 2. Pacote de Dados powerUDP (MENSAGEM)

Transporta a carga útil (payload) fragmentada palavra a palavra:

```c
typedef struct mensagem {
    int num_seq;                    // >0: Seq Válido; -1: Fim de Mensagem; -2: Seq Desativado
    char mensagem[256];             // Palavra individual
} MENSAGEM;
```

### 3. Confirmação de Receção (RESPONSE)

Efetua o feedback de controlo de fluxo do recetor para o emissor:

```c
typedef struct response {
    int num_seq;                    // Número de sequência confirmado
    char ans[5];                    // "ACK" ou "NACK"
} RESPONSE;
```

---

## Arquitetura do Servidor (`serverProj_edit.c`)

O servidor de controlo foi desenhado para ser concorrente e thread-safe.

```
                  +-----------------------+
                  |  accept() [Socket TCP]|
                  +-----------+-----------+
                              |
                              v
                   pthread_create(&thread_id)
                              |
              +---------------+---------------+
              |                               |
              v                               v
   sizeof(REGISTERMESSAGE)             sizeof(PEDIDO)
              |                               |
     register_client()               process_config_request()
              |                               |
  pthread_mutex_lock(&client_list_mutex) pthread_mutex_lock(&config_mutex)
```

### Destaques Técnicos da Implementação

- **Concorrência baseada em POSIX Threads:** Cada ligação cliente aceite via `accept()` gera uma thread *detached* dedicada (`handle_tcp_connection`).
- **Sincronização por Mutexes (`pthread_mutex_t`):**
  - `client_list_mutex`: Garante exclusão mútua ao inserir (`add_client`) ou remover (`remove_client`) nós na lista ligada concorrente (`CLIENTNODE* client_list`).
  - `config_mutex`: Protege a estrutura global `current_config` contra escritas/leituras simultâneas.
- **Autenticação PSK:** O registo só é efetuado se a `REGISTERMESSAGE.psk` coincidir com a constante `PSK` (`"minha_chave_secreta"`).
- **Difusão de Configuração Multicast:** Quando um cliente submete um `PEDIDO` de alteração, o servidor dispara uma thread de broadcast que envia a nova `CONFIGMESSAGE` para o grupo Multicast `239.0.0.1:12345` com `IP_MULTICAST_TTL = 3`.

---

## Arquitetura do Cliente (`cliente3.1_edit.c`)

O cliente implementa um modelo assíncrono e não-bloqueante para o utilizador:

### 1. Descobrimento de IP Local (`get_local_ip`)

Varre as interfaces de rede ativas (excluindo a loopback `lo`) recorrendo a `getifaddrs()`, garantindo que o socket UDP efetua o *bind* correto no endereço IP atribuído no ambiente GNS3.

### 2. Escuta Dinâmica Multicast (`wait_config`)

Executada numa thread em background assim que o protocolo é iniciado:

- Associa-se ao grupo multicast via `setsockopt(..., IP_ADD_MEMBERSHIP, &mreq, ...)`.
- Atualiza a variável global `CONFIGMESSAGE config` de forma transparente sempre que o servidor emite uma reconfiguração.

### 3. Emissão e Fragmentação (`request_point2_1`)

Ao enviar uma frase:

- O texto é tokenizado por espaços com `strtok()`.
- Cada palavra é transmitida individualmente pela função `send_message()`.
- É enviado um pacote final especial com `num_seq = -1` e mensagem `"FIM"` para assinalar a terminação da transmissão.

### 4. Receção Assíncrona e Registo (`request_point3`)

Thread dedicada à escuta no socket UDP. Constrói a mensagem original concatenando as palavras recebidas sequencialmente e, ao detetar o indicador `"FIM"`, escreve o resultado no ficheiro local `message_received.txt`.

---

## Protocolo powerUDP & Algoritmos de Fiabilidade

O algoritmo em `send_message()` implementa os seguintes mecanismos de controlo:

### 1. Recuo Exponencial (Exponential Backoff)

O tempo de espera por um ACK ajusta-se dinamicamente consoante o número de tentativas de retransmissão ($i$):

$$Timeout(i) = \text{BaseTimeout} \times 2^{i} \quad \text{(ms)}$$

No código:

```c
int timeout_ms = config.base_timeout;
if (config.enable_backoff) {
    timeout_ms *= pow(2, tentativa);
}
struct timeval timeout;
timeout.tv_sec = timeout_ms / 1000;
timeout.tv_usec = (timeout_ms % 1000) * 1000;
setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
```

### 2. Injeção de Perda de Pacotes (`inject_packet_loss`)

Para efeitos de teste de resiliência e avaliação de métricas, o cliente inclui uma simulação de perda estocástica no envio:

```c
int inject_packet_loss(int probability) {
    if (probability <= 0) return 0;
    if (probability >= 100) return 1;
    return (rand() % 100) < probability;
}
```

### 3. Validação de Sequência e Controlo ACK/NACK

- **Validação:** Se `num_seq == last_num_seq + 1`, a palavra é aceite e responde-se com `"ACK"`.
- **Duplicado / Fora de Ordem:** Se o número de sequência não for o esperado, responde-se com `"NACK"`, rejeitando a escrita repetida no buffer final.

---

## Cenário de Rede e NAT em GNS3

A validação do projeto decorre sobre uma infraestrutura virtualizada em GNS3:

- **Sub-redes:** 4 redes distintas interligadas por routers virtuais.
- **Mecanismos de NAT no Router do Servidor:**
  - **SNAT (Source NAT):** Traduz os endereços internos das máquinas para acesso à rede do servidor.
  - **DNAT (Destination NAT):** Redireciona o tráfego do porto TCP 443 externo para o endereço interno do servidor (`10.20.0.129:443`).

```
[Sub-rede Clientes] <---> [Router com SNAT/DNAT] <---> [Sub-rede Servidor (10.20.0.129)]
```
