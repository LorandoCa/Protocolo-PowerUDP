// PROJETO RC 2024/2025
// LORANDO TOME CA (PL8) & MIGUEL ANGELO LOURENCO FERNANDES (PL8)
//

// PONTOS ALTERADOS
// 1. o server agora utiliza threads para lidar com a ligacao dos clientes
// 2. o server usa uma thread especifica para enviar as novas configs aos clientes (broadcast)
// 3. o servidor agora tens semaforos para garantir que multiplas threads nao alteram a config atual simultaneamente

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/time.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <time.h>
#include <pthread.h>

typedef struct response{
    int num_seq;
    int ans[4]; //ACK ou NACK
} RESPONSE;

typedef struct ConfigMessage { 
    uint8_t enable_retransmission;  // 0 = Desativado, 1 = Ativado
    uint8_t enable_backoff;         // 0 = Desativado, 1 = Ativado
    uint8_t enable_sequence;        // 0 = Desativado, 1 = Ativado 
    uint16_t base_timeout;          // Tempo base para timeouts (ms) 
    uint8_t max_retries;             // Número máximo de retransmissões
} CONFIGMESSAGE;

typedef struct Pedido
{
    char nome[10];
    CONFIGMESSAGE payload;
} PEDIDO;

typedef struct RegisterMessage { 
char psk[64];  // Chave pré-definida para autenticação 
} REGISTERMESSAGE; 

typedef struct mensagem {
    int num_seq;

    char *mensagem;
} MENSAGEM;

int tcp_sock, udp_sock, multicast_sock;
struct sockaddr_in server_addr, client_addr;
struct sockaddr_in multicast_addr;

CONFIGMESSAGE current_config;
fd_set readfds;

#define MULTICAST_GROUP "239.0.0.1"
#define MULTICAST_PORT 12345
#define PORT 443
#define SERVER_IP "10.20.0.129"

int client_count = 0;


typedef struct CLIENTNODE {
    char nome[10];
    struct sockaddr_in addr;
    struct CLIENTNODE *next;
} CLIENTNODE;

#define BUFLEN 256
#define PSK "minha_chave_secreta"


CLIENTNODE *client_list = NULL; // lista de clientes ligados

pthread_mutex_t client_list_mutex = PTHREAD_MUTEX_INITIALIZER; // <--- nessessario visto que a lista de clientes vai ser usada pelas multiplas threads
pthread_mutex_t config_mutex = PTHREAD_MUTEX_INITIALIZER;

void init_tcp_server();
void init_multicast();
void* handle_tcp_connection(void *arg);
void register_client(int client_sock, REGISTERMESSAGE *reg);
void process_config_request(int client_sock, PEDIDO *req);
void *broadcast_config(void *arg);
void add_client(char *nome, struct sockaddr_in addr);
void remove_client(char *nome);



void erro(const char *msg) {
    perror(msg);
    fflush(stdout);
    exit(1);
}


int main() {
    // Config inicial
    current_config.enable_retransmission = 1;
    current_config.enable_backoff = 1;
    current_config.enable_sequence = 1;
    current_config.base_timeout = 1000;
    current_config.max_retries = 5;

    init_tcp_server();
    init_multicast();
    while(1) {
        socklen_t addr_len = sizeof(client_addr);
        int client_sock = accept(tcp_sock, (struct sockaddr *)&client_addr, &addr_len); // <--- aceitar conexao
    
        if (client_sock < 0) {
            perror("Erro ao aceitar conexão");
            continue;
        }
        
        pthread_t thread_id;
        int *client_sock_ptr = malloc(sizeof(int));
        *client_sock_ptr = client_sock;
        
        if(pthread_create(&thread_id, NULL, handle_tcp_connection, (void *)client_sock_ptr) != 0) { // <--- criar thread para lidar com cliente
            perror("Erro ao criar thread");
            close(client_sock);
            free(client_sock_ptr);
        }
        
        pthread_detach(thread_id); // <--- Termina a thread
    }

    return 0;
}

void init_tcp_server() {
    bzero((struct sockaddr *) &server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port=  htons((short)PORT);
    
    if (inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr) <= 0) {
        erro("Erro ao converter SERVER_IP");
    }

    if ((tcp_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        erro("Erro ao criar socket TCP");
    }

    if (bind(tcp_sock, (struct sockaddr *) &server_addr, sizeof(server_addr)) < 0) {
        erro("Erro ao vincular socket TCP");
    }

    if (listen(tcp_sock, 5) < 0) {
        erro("Erro ao escutar no socket TCP");
    }
}

void init_multicast() {
    if ((multicast_sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        erro("Erro ao criar socket multicast");
    }

    
    memset(&multicast_addr, 0, sizeof(multicast_addr));
    multicast_addr.sin_family = AF_INET;
    multicast_addr.sin_addr.s_addr = inet_addr(MULTICAST_GROUP);
    multicast_addr.sin_port = htons(MULTICAST_PORT);

    int reuse = 1;
    if (setsockopt(multicast_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        erro("Erro ao configurar SO_REUSEADDR");
    }

    int ttl=3;
    if (setsockopt(multicast_sock, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl)) < 0) {
        erro("Erro ao configurar TTL multicast");
    }

    if (bind(multicast_sock, (struct sockaddr *)&multicast_addr, sizeof(multicast_addr)) < 0) {
        erro("Erro ao vincular socket multicast");
    }
}

void *handle_tcp_connection(void *arg) {

    int client_sock= *(int*)arg;
    char buffer[BUFLEN];
    ssize_t bytes_read;

    // Ler mensagem recebida
    bytes_read = read(client_sock, buffer, sizeof(buffer));
    if (bytes_read <= 0) {
        close(client_sock);
        return NULL;
    }

    // Detetar se a mensagem e um registo ou um pedido
    if (bytes_read == sizeof(REGISTERMESSAGE)) { // <----- registo

        register_client(client_sock, (REGISTERMESSAGE *)buffer);

        bytes_read = read(client_sock, buffer, sizeof(buffer));
        if (bytes_read <= 0) {
            close(client_sock);
            return NULL;
        }

        process_config_request(client_sock, (PEDIDO *)buffer);

    } else if (bytes_read == sizeof(PEDIDO)) { // <----- pedido
        process_config_request(client_sock, (PEDIDO *)buffer);
    } else {                                // <---- sign out (cliente)
        remove_client(buffer);
    }

    close(client_sock);
    return NULL;
}

void register_client(int client_sock, REGISTERMESSAGE *reg) {
    char response[BUFLEN];

    // Verificar a PSK
    if (strcmp(reg->psk, PSK) != 0) {
        strcpy(response, "Denied");
        write(client_sock, response, strlen(response) + 1);
        return;
    }

    char client_name[10];
    snprintf(client_name, sizeof(client_name), "client%d", ++client_count);
    write(client_sock, client_name, strlen(client_name) + 1); // <---- enviar de volta ao cliente

    printf("Cliente registrado: %s\n", client_name);
    add_client(client_name, client_addr);
}

void process_config_request(int client_sock, PEDIDO *req) {
    pthread_mutex_lock(&config_mutex);
    current_config = req->payload;  // <--- Atualiza a config nova, e de seguida envia para os clientes
    pthread_mutex_unlock(&config_mutex);

    printf("Configuração atualizada por %s\n", req->nome);

    pthread_t one_time_thread;
    if(pthread_create(&one_time_thread, NULL, (void*)broadcast_config, NULL) != 0) {    // <--- Thread para lidar com o broadcast
        perror("Erro ao criar thread de broadcast");
    } else {
        pthread_detach(one_time_thread);
    }
}

void *broadcast_config(void *arg) {

    pthread_mutex_lock(&config_mutex);
    CONFIGMESSAGE config_to_send = current_config;  // <--- Atualiza a config nova, e de seguida envia para os clientes
    pthread_mutex_unlock(&config_mutex);

    if (sendto(multicast_sock, &config_to_send, sizeof(config_to_send), 0, (struct sockaddr *)&multicast_addr, sizeof(multicast_addr)) < 0) {
        perror("Erro ao enviar configuração multicast");
    } else {
        printf("Configuração transmitida via multicast\n");
    }

    return NULL;
}

void add_client(char *nome, struct sockaddr_in addr) {
    CLIENTNODE *new_node = (CLIENTNODE *)malloc(sizeof(CLIENTNODE));
    strncpy(new_node->nome, nome, sizeof(new_node->nome));
    new_node->addr = addr;
    
    pthread_mutex_lock(&client_list_mutex);
    new_node->next = client_list;
    client_list = new_node;
    pthread_mutex_unlock(&client_list_mutex);
}

void remove_client(char *nome) {
    pthread_mutex_lock(&client_list_mutex);
    CLIENTNODE **ptr = &client_list;
    while (*ptr != NULL) {
        if (strcmp((*ptr)->nome, nome) == 0) {
            CLIENTNODE *temp = *ptr;
            *ptr = (*ptr)->next;
            free(temp);
            break;
        }
        ptr = &(*ptr)->next;
    }
    client_count--;
    pthread_mutex_unlock(&client_list_mutex);
}