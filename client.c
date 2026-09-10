#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>
#include <math.h>
#include <netdb.h>
#include <ifaddrs.h>


typedef struct response{
    int num_seq;
    char ans[5]; //ACK ou NACK
}RESPONSE;

typedef struct ConfigMessage { 
uint8_t enable_retransmission;  // 0 = Desativado, 1 = Ativado
uint8_t enable_backoff;         // 0 = Desativado, 1 = Ativado
uint8_t enable_sequence;        // 0 = Desativado, 1 = Ativado 
uint16_t base_timeout;          // Tempo base para timeouts (ms) 
uint8_t max_retries;             // Número máximo de retransmissões
}CONFIGMESSAGE; 



typedef struct Pedido
{
    char nome[10];
    CONFIGMESSAGE payload;
}PEDIDO;

typedef struct RegisterMessage { 
char psk[64];  // Chave pré-definida para autenticação 
}REGISTERMESSAGE; 


int last_num_seq;
int num_seq;
int sim_erro=0;
int ACK;
double deliver_time;
int retransmission;

#define KEY "minha_chave_secreta"
#define BUFLEN 512	// Tamanho do buffer
#define PORT 444	// Porto para envio das mensagens
#define SERVER_IP "193.137.101.1"
#define MAX_MESSAGES 1024
#define MAX_MSG_SIZE 256 

#define MULTICAST_GROUP "239.0.0.1"
#define MULTICAST_PORT 12345


char *buf;
char nome[64];
int fd;


typedef struct {
    char str1[BUFLEN];
    char str2[BUFLEN];
} ThreadArgs;


typedef struct mensagem {
    int num_seq;

    char mensagem[MAX_MSG_SIZE];
} MENSAGEM;




CONFIGMESSAGE config;

//UDP
struct sockaddr_in si_minha, si_outra;
int s, recv_len, send_len;
socklen_t slen;

//TCP
struct sockaddr_in addr;
struct hostent *hostPtr;


struct ip_mreq mreq;

void erro(char *s);





char* get_local_ip() {
    struct ifaddrs *ifaddr, *ifa;
    static char ip[INET_ADDRSTRLEN];

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        return NULL;
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL) continue;

        if (ifa->ifa_addr->sa_family == AF_INET) {
            // Ignora loopback
            if (strcmp(ifa->ifa_name, "lo") != 0) {
                struct sockaddr_in *sa = (struct sockaddr_in *)ifa->ifa_addr;
                inet_ntop(AF_INET, &(sa->sin_addr), ip, INET_ADDRSTRLEN);
                break;
            }
        }
    }

    freeifaddrs(ifaddr);
    return ip[0] ? ip : NULL;
}




void *wait_config(void* arg) {
    int sc;
    struct sockaddr_in minha, remetente;
    struct ip_mreq mreq;
    socklen_t len = sizeof(remetente);
    CONFIGMESSAGE buffer;

    if ((sc = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1) {
        erro("Erro na criação do socket");
    }

    int reuse = 1;
    if (setsockopt(sc, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        erro("Erro ao definir SO_REUSEADDR");
    }

    memset(&minha, 0, sizeof(minha));
    minha.sin_family = AF_INET;
    minha.sin_addr.s_addr = htonl(INADDR_ANY);
    minha.sin_port = htons(MULTICAST_PORT); 

    if (bind(sc, (struct sockaddr*)&minha, sizeof(minha)) == -1) {
        erro("Erro no bind");
    }

    mreq.imr_multiaddr.s_addr = inet_addr(MULTICAST_GROUP);
    mreq.imr_interface.s_addr = htonl(INADDR_ANY); 
    if (setsockopt(sc, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
        erro("Erro ao entrar no grupo multicast");
    }

    while (1) {
        memset(&buffer, 0, sizeof(buffer));
        if (recvfrom(sc, &buffer, sizeof(buffer), 0, (struct sockaddr*)&remetente, &len) < 0) {
            erro("Erro ao receber dados");
        }

        config = buffer;
        printf("Config atualizada\n");
    }

    close(sc);
    return NULL ;
}


int inject_packet_loss(int probability);

void erro(char *s) {
    perror(s);
    exit(1);
}


int init_protocol(const char *server_ip, int server_port, const char *psk) {

    buf = (char *)malloc(BUFLEN);
    if (!buf) erro("malloc");

    // Resolve o IP do servidor
    if ((hostPtr = gethostbyname(server_ip)) == NULL)
        erro("Não consegui obter endereço");

    // Preenche o endereço do servidor
    bzero((void *)&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((short)server_port);
    bcopy((char *)hostPtr->h_addr, (char *)&addr.sin_addr, hostPtr->h_length);

    // Cria socket TCP
    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
        erro("socket");

    // Conecta ao servidor
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        erro("connect");

    

    // Prepara mensagem de registo
    REGISTERMESSAGE registo;
    memset(&registo, 0, sizeof(registo));
    strncpy(registo.psk, psk, sizeof(registo.psk) - 1);

    // Envia mensagem de registo
    write(fd, &registo, sizeof(registo));

    // Lê resposta do servidor
    ssize_t n = read(fd, buf, BUFLEN - 1);
    if (n < 0) erro("read");
    buf[n] = '\0';
    printf("Estou aqui3\n");

    if (strcmp(buf, "Denied") == 0) {
        printf("Sem permissao para registo\n");
        return 1;
    }

    // Guarda o nome devolvido pelo servidor
    strncpy(nome, buf, sizeof(nome) - 1);
    printf("%s\n", nome);

    return 0;
}



void close_protocol() {
    // Envia nome ao servidor para terminar ligação
    if (write(fd, nome, strlen(nome) + 1) < 0) {
        perror("Erro ao enviar mensagem para terminar");
    }

    // Fechar o socket TCP
    if (fd >= 0) {
        close(fd);
    }

    // Libertar memória de buf
    if (buf != NULL) {
        free(buf);
        buf = NULL;
    }
}


int request_protocol_config(int enable_retransmission, int enable_backoff, int enable_sequence, uint16_t base_timeout, uint8_t max_retries) {
    config.base_timeout = base_timeout;
    config.enable_backoff = enable_backoff;
    config.enable_retransmission = enable_retransmission;
    config.enable_sequence = enable_sequence;
    config.max_retries = max_retries;

    PEDIDO pedido;
    strncpy(pedido.nome, nome, sizeof(pedido.nome) - 1);
    pedido.nome[sizeof(pedido.nome) - 1] = '\0'; 
    pedido.payload = config;

    if (write(fd, &pedido, sizeof(pedido)) < 0) {
        perror("Erro ao enviar configuração");
        return 0;
    }

    return 1; // sucesso
}


void init_socket(){
    si_minha.sin_family = AF_INET;
    si_minha.sin_port = htons(PORT);
    char *res= get_local_ip();
    printf("%s\n", res);
    si_minha.sin_addr.s_addr = inet_addr(res);

    if ((s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1) {
        erro("Erro na criação do socket");
    }
    if (bind(s, (struct sockaddr*)&si_minha, sizeof(si_minha)) == -1) {
        erro("Erro no bind");
    }



}
void init_UDP(const char *destination, const int porta){
    si_outra.sin_family = AF_INET;
    si_outra.sin_port = htons(porta);
    si_outra.sin_addr.s_addr = inet_addr(destination);
    if (si_outra.sin_addr.s_addr == INADDR_NONE) {
        erro("Endereço IP inválido");
    }
}




int send_message(const char *destination, const char *message, int len){
    
    
    MENSAGEM pacote;
    strcpy(pacote.mensagem, message);
    if(!config.enable_sequence)pacote.num_seq= -2;
    else pacote.num_seq= num_seq;
    int tentativa=0;
    ACK=0;
    RESPONSE answer;
    do
    {   
        if(!inject_packet_loss(sim_erro)){
            if ((send_len = sendto(s, &pacote, sizeof(pacote), 0, (struct sockaddr*)&si_outra, sizeof(struct sockaddr_in))) == -1) {
                erro("Erro no sendto");
            }
        }
        printf("Message: %s sent\n", message);
        socklen_t slen = sizeof(struct sockaddr_in);
        int timeout_ms = config.base_timeout;
        if (config.enable_backoff) timeout_ms *= pow(2, tentativa);
        struct timeval timeout;
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        if ((recv_len = recvfrom(s, &answer, sizeof(answer), 0, (struct sockaddr*)&si_outra, &slen)) == -1) {
            if(config.enable_retransmission==0) break;
            tentativa++;
        }else{
            if ( strcmp(answer.ans,"ACK")==0 && answer.num_seq== pacote.num_seq )
            { 
                ACK= 1;
                retransmission+= tentativa;
            }
        }
    }while (ACK!= 1 && tentativa<config.max_retries);
    
    
    return 0;
}

int receive_message(char *buffer, int bufsize) {
    RESPONSE resposta;
    socklen_t slen = sizeof(si_outra);
    char mensagens_recebidas[MAX_MESSAGES][MAX_MSG_SIZE];
    MENSAGEM pacote;
    memset(&pacote, 0, sizeof(MENSAGEM));

        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 0;

        if (setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
            perror("Erro ao resetar SO_RCVTIMEO");
        }
        if ((recv_len = recvfrom(s, &pacote, sizeof(MENSAGEM), 0, (struct sockaddr *) &si_outra, &slen)) == -1) {
            perror("Erro no recvfrom");
            return 1;
        }
        if(pacote.num_seq==-1){
            strcpy(buffer, pacote.mensagem);
            resposta.ans[0] = 'A'; resposta.ans[1] = 'C'; resposta.ans[2] = 'K'; resposta.ans[3] = '\0';
            resposta.num_seq= pacote.num_seq;
            if (sendto(s, &resposta, sizeof(resposta), 0, (struct sockaddr *) &si_outra, slen) == -1) {
                perror("Erro ao enviar ACK/NACK");
            }
            return 1;
        }
        if(pacote.num_seq== last_num_seq+1 || pacote.num_seq==-2){
            strncpy(buffer, pacote.mensagem, bufsize);
            printf("Mensagem #%d recebida: %s\n", pacote.num_seq, pacote.mensagem);
            resposta.ans[0] = 'A'; resposta.ans[1] = 'C'; resposta.ans[2] = 'K'; resposta.ans[3] = '\0';
            resposta.num_seq= pacote.num_seq;
        }else{
            printf("Mensagem duplicada #%d recebida, enviando NACK.\n", pacote.num_seq);
            resposta.ans[0] = 'N'; resposta.ans[1] = 'A'; resposta.ans[2] = 'C'; resposta.ans[3] = 'K';
            resposta.num_seq= pacote.num_seq;
        }
        // Envia ACK OU NACK dependendo do ciclo acima
        if (sendto(s, &resposta, sizeof(resposta), 0, (struct sockaddr *) &si_outra, slen) == -1) {
            perror("Erro ao enviar ACK/NACK");
        }
    

    return 0;
}

void request_point1(){
    
    if(init_protocol(SERVER_IP,443,KEY)) return ;

    pthread_t thread;
    pthread_create(&thread,NULL,wait_config,NULL);
    pthread_detach(thread);

    int retrans, backoff, sequence;
    double timeout, max_retries;
    printf("Enable_retransmission? y or n\n");
    char aux,c;
    aux= getchar();
    while ((c = getchar()) != '\n' && c != EOF);
    if(aux=='y'){
        retrans= 1; 
        printf("Base timeout:");
        char aux1[MAX_MSG_SIZE];
        fgets(aux1,MAX_MSG_SIZE, stdin);
        char *endptr;
        timeout = strtod(aux1, &endptr);
        printf("Retries number:");
        fgets(aux1,MAX_MSG_SIZE, stdin);
        max_retries= strtod(aux1, &endptr);
    }

    else if(aux=='n') retrans= 0; 
    else{ printf("Not possible"); exit(0);};

    printf("Enable_backoff? y or n\n");
    aux= getchar();
    while ((c = getchar()) != '\n' && c != EOF);
    if(aux=='y') backoff= 1; else if(aux=='n') backoff= 0; else{ printf("Not possible"); exit(0);};
    printf("Enable_sequence? y or n\n");
    aux= getchar();
    while ((c = getchar()) != '\n' && c != EOF);
    if(aux=='y') sequence= 1; else if(aux=='n') sequence= 0; else{ printf("Not possible"); exit(0);};
    
    request_protocol_config(retrans, backoff, sequence, timeout, max_retries);
    
    printf("Pedido feito com sucesso\n");
    return ;

}


void *request_point2_1(void* arg);

int request_point2(){
    retransmission=0;
    deliver_time=0;
    char message[MAX_MSG_SIZE];
    char dest_ip[MAX_MSG_SIZE];
    char str_port[MAX_MSG_SIZE];
    printf("Insira a mensagem que quer enviar:");
    fgets(message,MAX_MSG_SIZE,stdin);
    printf("Insira o ip de destino:");
    fgets(dest_ip, MAX_MSG_SIZE, stdin);
    dest_ip[strlen(dest_ip)-1]='\0';
    printf("Insira a porta de envio:");
    fgets(str_port, MAX_MSG_SIZE, stdin);
    str_port[strlen(str_port)-1]='\0';
    int porta = atoi(str_port);
    printf("%s    %d\n", dest_ip,porta);
    init_UDP(dest_ip, porta);
    ThreadArgs *aux = malloc(sizeof(ThreadArgs));
    strcpy(aux->str1,message);
    strcpy(aux->str2, dest_ip);
    pthread_t thread;
    pthread_create(&thread,NULL,request_point2_1,(void*)aux);
    pthread_detach(thread);
    return 0;

}

void *request_point2_1(void* arg){
    time_t start, end;
    ThreadArgs *args = (ThreadArgs *)arg;
    num_seq=1;
    time(&start);
    char *palavra = strtok(args->str1, " ");
    while (palavra != NULL) {
        send_message(args->str2, palavra, strlen(palavra));
        palavra = strtok(NULL, " ");
        num_seq++;
    }
    time(&end);
    deliver_time= difftime(end, start);
    num_seq=-1;
    char message[BUFLEN];
    strcpy(message, "FIM");
    int res=send_message(args->str2,message,strlen(message));
    return NULL;
}




void* request_point3( void *arg){
    char final_message[MAX_MESSAGES * MAX_MSG_SIZE];
    memset(final_message, 0, sizeof(final_message));
    char aux[MAX_MSG_SIZE];
    last_num_seq=0;
    while(strcmp(aux,"FIM")!=0){
        int res=receive_message(aux, MAX_MSG_SIZE);
        if (config.enable_sequence) last_num_seq++;
        strncat(final_message, aux, sizeof(final_message) - strlen(aux) - 1);
    }
    FILE *file= fopen("message_received.txt", "a+");
    if (file == NULL) {
        printf("Erro ao abrir o ficheiro message_received.txt .\n");
        return NULL;
    }
    final_message[strlen(final_message)+1]= '\0';
    fputs(final_message,file);
    fclose(file);
    return NULL;
}


int get_last_message_stats(int *retransmissions, double *delivery_time){
    *retransmissions= retransmission;
    *delivery_time= deliver_time;
    return 0;
}

int inject_packet_loss(int probability) {
    if (probability <= 0) return 0;    // Nunca perde pacote
    if (probability >= 100) return 1;  // Sempre perde pacote

    int r = rand() % 100;
    return r < probability;
}




int main(void) {
    init_socket();
    srand((unsigned int)time(NULL));

    char c;
    while (1){
        printf("1.Request Protocol_config\n2.Send_message\n3.Recieve message\n4.Get_last_message_stats\n5.Inject packet loss\n6.SAIR\n");
        char option = getchar();
        while ((c = getchar()) != '\n' && c != EOF);

        switch (option) {
            case '1': {

                request_point1();
                break;
            }
            case '2':
                num_seq = 0;
                request_point2();
                break;

            case '3': {
                pthread_t thread;
                pthread_create(&thread, NULL, request_point3, NULL);
                pthread_detach(thread);
                break;
            }
            case '4': {
                int RETRANS;
                double DELIVERY;
                get_last_message_stats(&RETRANS, &DELIVERY);
                printf("Last message information:\nRetransmissions:%d\nDelivery time:%lf\n", RETRANS, DELIVERY);
                break;
            }
            case '5': {
                printf("Insira a probabilidade de perda de pacotes (entre 0 e 100): ");
                char aux1[MAX_MSG_SIZE];
                fgets(aux1, MAX_MSG_SIZE, stdin);
                sim_erro = atoi(aux1); // Corrigido: atoi mais direto
                break;
            }
            case '6':
                close_protocol();
                printf("Encerrando programa.\n");
                exit(0);
            default:
                printf("Opção inválida.\n");
        }
    }

    return 0;
}
//threads porque os valores sao partilhados entre elas e nao é preciso usar memoria partilhada
